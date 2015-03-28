//
//  MapLiteTableMgr.swift
//  ClockCommander
//
//  Created by Jacob Taylor on 3/21/15.
//  Copyright (c) 2015 jaketaylorpro. All rights reserved.
//

import Foundation
public class MapLiteTableMgr {
    
    internal let getTablesStatement:MapLiteStatement<DbTable>
    internal let getTablesMapper:MapLiteMapper<DbTable>
    internal let db:MapLite
    public init(db:MapLite) {
        self.db=db
        self.getTablesStatement=self.db.prepareQuery("select name from sqlite_master where type='table';")
        let create = {()->DbTable in return DbTable()}
        let nameSetter={(var t:DbTable,v:MapLiteValue) -> DbTable in
            switch v {
            case .MapLiteText(let v_text):
                t.name = v_text;return t
            default:
                assert(false,"invalid argument in setter")
            }
        }
        let map=["name":(MapLiteType.SQLITE_TEXT,MapLiteIdentity.NormalColumn,nameSetter)]
        self.getTablesMapper=MapLiteMapper(create: create, maps: map)
    }
    public func getTableNames() -> [String] {
        return db.query(getTablesStatement,mapper:getTablesMapper).map({(t:DbTable)->String in return t.name})
    }
    public func ensureTables(tables:[String:String]) {
        let existingTables=self.getTableNames();
        let tablesNamesToCreate=tables.keys.filter({(s:String)->Bool in return !contains(existingTables,s)})
        for tn in tablesNamesToCreate {
            let stmt=self.db.prepareExec(tables[tn]!)
            self.db.exec(stmt)
        }
    }
    public func dropAllTables() {
        let existingTables=self.getTableNames();
        for tn in existingTables {
            self.db.exec(self.db.prepareExec("drop table "+tn+";"))
        }
    }
    internal class DbTable {
        internal var name:String = ""
    }
}