//
//  Update.swift
//  PostgresStORM
//
//  Created by Jonathan Guthrie on 2016-09-24.
//
//

import StORM
import PerfectLogger

/// Extends the main class with update functions.
extension PostgresStORM {

	/// Updates the row with the specified data.
	/// This is an alternative to the save() function.
	/// Specify matching arrays of columns and parameters, as well as the id name and value.
	@discardableResult
	public func update(cols: [String], params: [Any?], idName: String, idValue: Any) throws -> Bool {

		var paramsWithId = params
		var set = [String]()
		for i in 0..<params.count {
			
			set.append("\"\(cols[i].lowercased())\" = $\(i+1)")
		}
		paramsWithId.append(idValue)

		let str = "UPDATE \(self.table()) SET \(set.joined(separator: ", ")) WHERE \"\(idName.lowercased())\" = $\(params.count+1)"

		do {
			try exec(str, params: paramsWithId)
		} catch {
			LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
			self.error = StORMError.error("\(error)")
			throw error
		}

		return true
	}

	/// Updates the row with the specified data.
	/// This is an alternative to the save() function.
	/// Specify a [(String, Any)] of columns and parameters, as well as the id name and value.
	@discardableResult
	public func update(data: [(String, Any?)], idName: String = "id", idValue: Any) throws -> Bool {

		var keys = [String]()
		var vals = [Any?]()
		for i in 0..<data.count {
			keys.append(data[i].0.lowercased())
			vals.append(data[i].1)
		}
		do {
			return try update(cols: keys, params: vals, idName: idName, idValue: idValue)
		} catch {
			LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}

}
