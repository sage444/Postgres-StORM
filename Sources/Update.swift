//
//  Update.swift
//  PostgresStORM
//
//  Created by Jonathan Guthrie on 2016-09-24.
//
//

import Foundation
import StORM
import PostgreSQL

// TODO:  detect response and return t/f as appropriate

extension PostgresStORM {

	public func update(_
			cols: [String],
			vals: [Any],
			idName: String = "id",
			limit: Int = 1
		) -> Bool {

		// PostgreSQL specific insert staement exec
		var valsString = [String]()
		var substString = [String]()
		for i in 0..<vals.count {
			valsString.append(String(describing: vals[i]))
			substString.append("$\(i+1)")
		}

		// MAKE INTO A MAP

//		let _ = exec(str, params: valsString)

		return true
	}


	public func update(_
		   cols: [(String, Any)],
		   idName: String = "id",
		   limit: Int = 1
		) -> Bool {

		// PostgreSQL specific insert staement exec
//		var valsString = [String]()
//		var substString = [String]()
//		for i in 0..<vals.count {
//			valsString.append(String(describing: vals[i]))
//			substString.append("$\(i+1)")
//		}

		// MAKE INTO A MAP

		//		let _ = exec(str, params: valsString)
		
		return true
	}

}