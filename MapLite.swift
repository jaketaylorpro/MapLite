import Foundation
/*
//codes
let SQLITE_OK=Int32(0)
let SQLITE_ROW=Int32(100)
let SQLITE_DONE=Int32(101)
//types
let SQLITE_INTEGER=Int32(1)
let SQLITE_FLOAT=Int32(2)
let SQLITE_TEXT=Int32(3)
*/
public class MapLite {
    internal let dbName:String
    internal let dbPath:String
    internal let mustInit:Bool
    internal var ccdb: COpaquePointer = nil
    public init(dbName:String) {
        self.dbName=dbName
        //open DB
        let filemgr = NSFileManager.defaultManager()
        let dirPaths = NSSearchPathForDirectoriesInDomains(
            .DocumentDirectory,
            .UserDomainMask,
            true)
        let docsDir = dirPaths[0] as! String
        self.dbPath = docsDir.stringByAppendingPathComponent(dbName)
        self.mustInit = !filemgr.fileExistsAtPath(dbPath)
        let ret_open=sqlite3_open(dbPath,&self.ccdb)//TODO handle return code (should be OK:0)
        assert(ret_open==SQLITE_OK, "sqlite3_open returned an unexpected return code")
    }
    
    public func prepareQuery<T>(sql:String) -> MapLiteStatement<T> {
        return MapLiteStatement<T>(sql:sql,db:self)
    }
    public func prepareExec(sql:String) -> MapLiteStatementVoid {
        return MapLiteStatementVoid(sql:sql,db:self)
    }
    public func exec(stmt:MapLiteStatementVoid){
        let ret_step=sqlite3_step(stmt.stmt)
        if ret_step == SQLITE_ROW {
            assert(false,"rows returned in exec")
        }
        else if ret_step != SQLITE_DONE {
            assert(false,"sqlite3_step returned an unexpected return code")
        }
        let ret_reset:Int32=sqlite3_reset(stmt.stmt)
        assert(ret_reset == SQLITE_OK,"sqlite3_reset returned am unexpected return code")
    }
    public func insert<T>(insertStmt:MapLiteStatement<T>,values:[String:MapLiteValue],selectStmt:MapLiteStatement<T>,mapper:MapLiteMapper<T>) -> T {
        //bind insert parameters
        let paramCount=sqlite3_bind_parameter_count(insertStmt.stmt)
        assert(values.count == Int(paramCount),"parameter map count did not match the number of parameter in the statement")
        if paramCount > 0 {
            for i in 1...paramCount {
                let paramNamePtr = sqlite3_bind_parameter_name(insertStmt.stmt,i);
                let paramName = String.fromCString(UnsafePointer<Int8>(paramNamePtr))
                let value = values[paramName!]
                var ret_bind:Int32 = (-1)
                switch value! {
                case .MapLiteInt(let int_value):
                    ret_bind = sqlite3_bind_int(insertStmt.stmt, i as Int32, Int32(int_value))
                case .MapLiteFloat(let float_value):
                    ret_bind = sqlite3_bind_double(insertStmt.stmt,i as Int32,Double(float_value))
                case .MapLiteText(let text_value):
                    let ptr = text_value
                    let len = text_value.lengthOfBytesUsingEncoding(NSUTF8StringEncoding)
                    let destruct = SQLITE_STATIC
                    ret_bind = sqlite3_bind_text(insertStmt.stmt, i as Int32, ptr,Int32(len),destruct)
                default:
                    assert(false,"unimplemented mapping type")
                }
                assert(ret_bind == SQLITE_OK,"sqlite3_bind returned am unexpected return code")
            }
        }
        //execute the insert statement, it should return done
        let ret_step_insert=sqlite3_step(insertStmt.stmt)
        assert(ret_step_insert == SQLITE_DONE,"sqlite_step returned an unexpected result")
        //get the last id
        let lastId=sqlite3_last_insert_rowid(self.ccdb)
        //bind the last id to the selectOne statement and execute it
        let ret_bind_lastid=sqlite3_bind_int(selectStmt.stmt, Int32(1), Int32(lastId))
        assert(ret_bind_lastid == SQLITE_OK,"sqlite3_bind returned am unexpected return code when binding the lastrowid")
        let ret_step_select1=sqlite3_step(selectStmt.stmt)
        assert(ret_step_select1 == SQLITE_ROW,"sqlite_step returned an unexpected result")
        //map the row
        let row=self.mapRow(selectStmt, mapper: mapper)
        //run once more, and it should return done
        let ret_step_select2=sqlite3_step(selectStmt.stmt)
        assert(ret_step_select2 == SQLITE_DONE,"second invocation of sqlite3_step returned an unexpected return code")
        //reset the select statement
        let ret_reset_insert:Int32=sqlite3_reset(insertStmt.stmt)
        assert(ret_reset_insert == SQLITE_OK,"sqlite3_reset returned an unexpected return code")
        let ret_reset_select:Int32=sqlite3_reset(selectStmt.stmt)
        assert(ret_reset_select == SQLITE_OK,"sqlite3_reset returned an unexpected return code")
        return row
    }
    public func query<T>(stmt:MapLiteStatement<T>,mapper:MapLiteMapper<T>) -> [T] {
        var ret_step=SQLITE_ROW
        var rows:[T]=[]
        while ret_step == SQLITE_ROW {
            ret_step = sqlite3_step(stmt.stmt)
            if ret_step == SQLITE_ROW {
                rows.append(self.mapRow(stmt,mapper: mapper))
            }
            else {
                assert(ret_step == SQLITE_DONE,"sqlite3_step returned an unexpected return code")
            }
        }
        let ret_reset:Int32=sqlite3_reset(stmt.stmt)
        assert(ret_reset==SQLITE_OK,"sqlite3_reset returned an unexpected return code")
        return rows

    }
    private func mapRow<T>(stmt: MapLiteStatement<T>,mapper: MapLiteMapper<T>) -> T {
        var row=mapper.create()
        let colCount=sqlite3_column_count(stmt.stmt)
        assert(colCount>0,"sqlite3_column_count returned 0 columns")
        for i in 0...colCount-1 {
            let colNamePtr=sqlite3_column_name(stmt.stmt,i)
            let colName=String.fromCString(UnsafePointer<Int8>(colNamePtr))
            //let col_type=sqlite3_column_name(stmt.stmt,i)
            if let (ty,ident,fn)=mapper.maps[colName!] {
                switch ty {
                    case .SQLITE_INTEGER:
                        let intVal=Int(sqlite3_column_int(stmt.stmt,i))
                        row=fn(row,.MapLiteInt(intVal))
                    case .SQLITE_DATE:
                        let textValPtr = sqlite3_column_text(stmt.stmt,i)
                        let textVal = String.fromCString(UnsafePointer<Int8>(textValPtr))
                        let formatter = NSDateFormatter();formatter.dateFormat="yyyy-MM-dd HH:mm:ss"
                        let date:NSDate = formatter.dateFromString(textVal!)!
                        row=fn(row,.MapLiteDate(date))
                    case .SQLITE_FLOAT:
                        let floatVal=Float(sqlite3_column_double(stmt.stmt,i))
                        row=fn(row,.MapLiteFloat(floatVal))
                    case .SQLITE_TEXT:
                        let textValPtr=sqlite3_column_text(stmt.stmt,i)
                        let textVal=String.fromCString(UnsafePointer<Int8>(textValPtr))
                        row=fn(row,.MapLiteText(textVal!))
                    default:
                        assert(false,"unimplemented mapping type")
                }
            }
        }
        return row
    }
}
public class MapLiteStatement<T> {
    internal let sql:String
    internal var stmt: COpaquePointer = nil
    internal init(sql:String,db:MapLite) {
        self.sql=sql //optionally add semicolon here
        let sqlLength=Int32(self.sql.lengthOfBytesUsingEncoding(NSUTF8StringEncoding))
        var unusedPointer=UnsafeMutablePointer<UnsafePointer<Int8>>.alloc(1)
        let ret_prepare_v2=sqlite3_prepare_v2(db.ccdb,self.sql,sqlLength,&self.stmt,unusedPointer)
        assert(ret_prepare_v2==SQLITE_OK, "sqlite3_prepare_v2 returned an unexpected return code")
    }
}
public typealias MapLiteStatementVoid=MapLiteStatement<()>
public class MapLiteMapper<T> {
    internal let maps:Dictionary<String,(MapLiteType,MapLiteIdentity,(T,MapLiteValue)->T)>
    let create:()->T
    init(create:()->T,maps:Dictionary<String,(MapLiteType,MapLiteIdentity,(T,MapLiteValue)->T)>) {
        self.create=create
        self.maps=maps
    }
}
public enum MapLiteValue {
    case MapLiteInt(Int)
    case MapLiteText(String)
    case MapLiteFloat(Float)
    case MapLiteDate(NSDate)
}
public enum MapLiteType: Int32 {
    case SQLITE_INTEGER = 1
    case SQLITE_FLOAT   = 2
    case SQLITE_TEXT    = 3
    case SQLITE_BLOB    = 4
    case SQLITE_NULL    = 5
    case SQLITE_DATE    = 6
}
public enum MapLiteIdentity {
    case IdentityColumn
    case NormalColumn
}
let SQLITE_STATIC = sqlite3_destructor_type(COpaquePointer(bitPattern: 0))
let SQLITE_TRANSIENT = sqlite3_destructor_type(COpaquePointer(bitPattern: -1))