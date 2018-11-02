//
//  PostgreStORM.swift
//  PostgresSTORM
//
//  Created by Jonathan Guthrie on 2016-10-03.
//
//

import StORM
import PerfectPostgreSQL
import PerfectLogger
import Foundation

/// PostgresConnector sets the connection parameters for the PostgreSQL Server access
/// Usage:
/// PostgresConnector.host = "XXXXXX"
/// PostgresConnector.username = "XXXXXX"
/// PostgresConnector.password = "XXXXXX"
/// PostgresConnector.port = 5432
public struct PostgresConnector {

	public static var host: String		= ""
	public static var username: String	= ""
	public static var password: String	= ""
	public static var database: String	= ""
	public static var port: Int			= 5432

	public static var quiet: Bool		= false

	private init(){}

}

/// SuperClass that inherits from the foundation "StORM" class.
/// Provides PosgreSQL-specific ORM functionality to child classes
open class PostgresStORM: StORM, StORMProtocol {

	/// Table that the child object relates to in the database.
	/// Defined as "open" as it is meant to be overridden by the child class.
	open func table() -> String {
		let m = Mirror(reflecting: self)
		return ("\(m.subjectType)").lowercased()
	}

	/// Empty initializer
	override public init() {
		super.init()
	}

    private func printDebug(_ statement: String, _ params: [Any?]) {
        if StORMdebug {
            let strParams = params.map { p -> String in
                guard let p = p else {
                    return "NULL"
                }
                let m = Mirror(reflecting: p)
                if m.displayStyle == .optional, let value = m.children.first?.value {
                    return String(describing: value)
                }
                return String(describing: p)
            }
            LogFile.debug("StORM Debug: \(statement) : \(strParams.joined(separator: ", "))", logFile: "./StORMlog.txt")
        }
    }

	// Internal function which executes statements, with parameter binding
	// Returns raw result
	@discardableResult
	func exec(_ statement: String, params: [Any?]) throws -> PGResult {
		let thisConnection = PostgresConnect(
			host:		PostgresConnector.host,
			username:	PostgresConnector.username,
			password:	PostgresConnector.password,
			database:	PostgresConnector.database,
			port:		PostgresConnector.port
		)

		thisConnection.open()
		if thisConnection.state == .bad {
			error = .connectionError
			throw StORMError.error("Connection Error")
		}
		thisConnection.statement = statement
        var unwrapped = Array<Any?>.init(repeating: nil, count: params.count)
		printDebug(statement, params)
        for i in 0..<unwrapped.count {
            guard let p = params[i] else {
                continue
            }
            let m = Mirror(reflecting: p)
            if m.displayStyle == .optional {
                if let v = m.children.first?.value {
                    unwrapped[i] = v
                    continue
                }
                continue
            }
            unwrapped[i] = p
        }

        printDebug(statement, unwrapped)
		let result = thisConnection.server.exec(statement: statement, params: unwrapped)

		// set exec message
		errorMsg = thisConnection.server.errorMessage().trimmingCharacters(in: .whitespacesAndNewlines)
		if StORMdebug { LogFile.info("Error msg: \(errorMsg)", logFile: "./StORMlog.txt") }
		if isError() {
			thisConnection.server.close()
			throw StORMError.error(errorMsg)
		}
		thisConnection.server.close()
		return result
	}

	// Internal function which executes statements, with parameter binding
	// Returns a processed row set
	@discardableResult
	func execRows(_ statement: String, params: [String]) throws -> [StORMRow] {
		let thisConnection = PostgresConnect(
			host:		PostgresConnector.host,
			username:	PostgresConnector.username,
			password:	PostgresConnector.password,
			database:	PostgresConnector.database,
			port:		PostgresConnector.port
		)

		thisConnection.open()
		if thisConnection.state == .bad {
			error = .connectionError
			throw StORMError.error("Connection Error")
		}
		thisConnection.statement = statement

		printDebug(statement, params)
		let result = thisConnection.server.exec(statement: statement, params: params)

		// set exec message
		errorMsg = thisConnection.server.errorMessage().trimmingCharacters(in: .whitespacesAndNewlines)
		if StORMdebug { LogFile.info("Error msg: \(errorMsg)", logFile: "./StORMlog.txt") }
		if isError() {
			thisConnection.server.close()
			throw StORMError.error(errorMsg)
		}

		let resultRows = parseRows(result)
		//		result.clear()
		thisConnection.server.close()
		return resultRows
	}


	func isError() -> Bool {
		if errorMsg.contains(string: "ERROR"), !PostgresConnector.quiet {
			print(errorMsg)
			return true
		}
		return false
	}


	/// Generic "to" function
	/// Defined as "open" as it is meant to be overridden by the child class.
	///
	/// Sample usage:
	///		id				= this.data["id"] as? Int ?? 0
	///		firstname		= this.data["firstname"] as? String ?? ""
	///		lastname		= this.data["lastname"] as? String ?? ""
	///		email			= this.data["email"] as? String ?? ""
	open func to(_ this: StORMRow) {
	}

	/// Generic "makeRow" function
	/// Defined as "open" as it is meant to be overridden by the child class.
	open func makeRow() {
		guard self.results.rows.count > 0 else {
			return
		}
		self.to(self.results.rows[0])
	}

	/// Standard "Save" function.
	/// Designed as "open" so it can be overriden and customized.
	/// If an ID has been defined, save() will perform an updae, otherwise a new document is created.
	/// On error can throw a StORMError error.

	open func save() throws {
		do {
			if keyIsEmpty() {
				try insert(asDataOptional(1))
			} else {
				let (idname, idval) = firstAsKey()
				try update(data: asDataOptional(1), idName: idname, idValue: idval)
			}
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}

	/// Alternate "Save" function.
	/// This save method will use the supplied "set" to assign or otherwise process the returned id.
	/// Designed as "open" so it can be overriden and customized.
	/// If an ID has been defined, save() will perform an updae, otherwise a new document is created.
	/// On error can throw a StORMError error.

	open func save(set: (_ id: Any)->Void) throws {
		do {
			if keyIsEmpty() {
				let setId = try insert(asDataOptional(1))
				set(setId)
			} else {
				let (idname, idval) = firstAsKey()
				try update(data: asDataOptional(1), idName: idname, idValue: idval)
			}
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}

	/// Unlike the save() methods, create() mandates the addition of a new document, regardless of whether an ID has been set or specified.
	override open func create() throws {
		do {
            _ = try insert(asDataOptional())
		} catch {
			LogFile.error("Error: \(error)", logFile: "./StORMlog.txt")
			throw StORMError.error("\(error)")
		}
	}


	/// Table Creation (alias for setup)

	open func setupTable(_ str: String = "") throws {
		try setup(str)
	}

	/// Table Creation
	/// Requires the connection to be configured, as well as a valid "table" property to have been set in the class

    open func setup(_ str: String = "") throws {
        LogFile.info("Running setup: \(table())", logFile: "./StORMlog.txt")
        var createStatement = str
        if str.count == 0 {
            var opt = [String]()
            var keyName = ""
            for child in Mirror(reflecting: self).children {
                guard let key = child.label, !key.hasPrefix("internal_"), !key.hasPrefix("_") else {
                    continue
                }
                let m = Mirror(reflecting: child.value)
                var isOptional = false
                if let displayStyle = m.displayStyle, displayStyle == .optional {
                    isOptional = true
                }
                
                var verbage = "\(key) "
                if opt.count == 0 && [Int.self].contains { $0 == m.subjectType }  {
                    verbage += "bigserial"
                } else if [Int.self, Int?.self].contains(where: { $0 == m.subjectType }) {
                    verbage += "integer"
                } else if [Bool.self, Bool?.self].contains(where: { $0 == m.subjectType }) {
                    verbage += "boolean"
                } else if  [Double.self, Double?.self].contains(where: { $0 == m.subjectType }) {
                    verbage += "double precision"
                } else if [UInt8.self, UInt8?.self, UInt16.self, UInt16?.self].contains(where: { $0 == m.subjectType }) {
                    verbage += "smallint"
                } else if  [UInt32.self, UInt32?.self].contains(where: { $0 == m.subjectType }) {
                    verbage += "integer"
                } else if [UInt.self, UInt?.self, UInt64.self, UInt64?.self].contains(where: { $0 == m.subjectType }) {
                    verbage += "bigint"
                } else if [Date.self, Date?.self].contains(where: { $0 == m.subjectType }) {
                    verbage += "timestamp"
                } else {
                    verbage += "text"
                }
                if opt.count == 0 {
                    verbage += " NOT NULL"
                    keyName = key
                }
                if isOptional {
                    verbage += " NULL"
                }
                opt.append(verbage)
                
            }
            let keyComponent = ", CONSTRAINT \(table())_key PRIMARY KEY (\(keyName)) NOT DEFERRABLE INITIALLY IMMEDIATE"
            
            createStatement = "CREATE TABLE IF NOT EXISTS \(table()) (\(opt.joined(separator: ", "))\(keyComponent));"
            if StORMdebug { LogFile.info("createStatement: \(createStatement)", logFile: "./StORMlog.txt") }
            
        }
        do {
            try sql(createStatement, params: [])
        } catch {
            LogFile.error("Error msg: \(error)", logFile: "./StORMlog.txt")
            throw StORMError.error("\(error)")
        }
	}

    open func modifyValue(_ v: Any?, forKey k: String) -> Any? {
        return v
    }
    
    open func asDataOptional(_ offset: Int = 0) -> [(String, Any?)] {
        var c = [(String, Any?)]()
        var count = 0
        let mirror = Mirror(reflecting: self)
        for case let (label?, value) in mirror.children {
            if count >= offset && !label.hasPrefix("internal_") && !label.hasPrefix("_") {
                if value is [String:Any] {
                    c.append((label, modifyValue(try! (value as! [String:Any]).jsonEncodedString(), forKey: label)))
                } else if value is [String] {
                    c.append((label, modifyValue((value as! [String]).joined(separator: ","), forKey: label)))
                } else {
                    c.append((label, modifyValue(value, forKey: label)))
                }
            }
            count += 1
        }
        return c
    }
}


