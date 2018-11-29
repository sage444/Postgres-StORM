//
//  Delete.swift
//  PostgresStORM
//
//  Created by Jonathan Guthrie on 2016-09-24.
//
//

import PerfectLib
import StORM
import PerfectLib
import Foundation

/// Performs delete-specific functions as an extension
extension PostgresStORM {

	func deleteSQL(_ table: String, idName: String = "id") -> String {
		return "DELETE FROM \(table) WHERE \"\(idName.lowercased())\" = $1"
	}

	/// Deletes one row, with an id as an integer
	@discardableResult
	public func delete(_ id: Int, idName: String = "id") throws -> Bool {
		do {
			try exec(deleteSQL(self.table(), idName: idName), params: [String(id)])
		} catch {
			Log.error(message:"Error: \(error)")
			self.error = StORMError.error("\(error)")
			throw error
		}
		return true
	}

	/// Deletes one row, with an id as a String
	@discardableResult
	public func delete(_ id: String, idName: String = "id") throws -> Bool {
		do {
			try exec(deleteSQL(self.table(), idName: idName), params: [id])
		} catch {
			Log.error(message:"Error: \(error)")
			self.error = StORMError.error("\(error)")
			throw error
		}
		return true
	}

	/// Deletes one row, with an id as a UUID
	@discardableResult
	public func delete(_ id: Foundation.UUID, idName: String = "id") throws -> Bool {
		do {
			try exec(deleteSQL(self.table(), idName: idName), params: [id.uuidString])
		} catch {
			Log.error(message:"Error: \(error)")
			self.error = StORMError.error("\(error)")
			throw error
		}
		return true
	}

}
