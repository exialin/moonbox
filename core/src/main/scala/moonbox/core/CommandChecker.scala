/*-
 * <<
 * Moonbox
 * ==
 * Copyright (C) 2016 - 2019 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */

package moonbox.core

import moonbox.common.MbLogging
import moonbox.core.command._

// 在MoonboxSession.parsedCommand方法中被调用，检查用户权限
// 这里检查的都是Moonbox扩展的命令，不包括SELECT
object CommandChecker extends MbLogging {

	private def require(condition: Boolean, message: String): Unit = {
		if (!condition) throw new Exception(message)
	}

	def check(cmd: MbCommand, catalog: MoonboxCatalog): Unit = {
		cmd match {
			case org: Organization =>
				require(isRoot, "Only ROOT can do this command.")
			case other =>
				// ROOT只能进行organization相关的操作，不能进行查询
				require(!isRoot,
					"ROOT can only do organization relative commands.")
				notOrganization(other, catalog)
		}

		def isRoot: Boolean = {
			catalog.getCurrentOrg.equalsIgnoreCase("SYSTEM") &&
				catalog.getCurrentUser.equalsIgnoreCase("ROOT")
		}
	}

	private def notOrganization(cmd: MbCommand, catalog: MoonboxCatalog): Unit = cmd match {
		case account: Account =>
      // CREATE/ALTER/DROP/DESC/SHOW USER等USER相关命令需要有Account权限
			require(catalog.catalogUser.account, "Permission denied.")

		case ddl: DDL =>
			require(catalog.catalogUser.ddl, "Permission denied.")

		// DML语句现在没有检查权限！包括USER、SHOW、DESC命令
		case dml: DML =>

		// GRANT/REVOKE SELECT 命令
		case GrantResourceToUser(_, _, _)
				 | RevokeResourceFromUser(_, _, _)
				 | GrantResourceToGroup(_, _, _)
				 | RevokeResourceFromGroup(_, _, _) =>

			require(catalog.catalogUser.dcl, "Permission denied.")

		// 只有SA能执行GRANT GRANT 语句
		case GrantGrantToUser(_, _)
				 | RevokeGrantFromUser(_, _)
				 | GrantGrantToGroup(_, _)
				 | RevokeGrantFromGroup(_, _) =>

			require(catalog.catalogUser.isSA, "Permission denied.")

		// GRANT {ACCOUNT | DDL | DCL} 命令
		case GrantPrivilegeToUser(privileges, _)  =>

			require(
				privileges.map {
					case RolePrivilege.ACCOUNT => catalog.catalogUser.grantAccount
					case RolePrivilege.DDL => catalog.catalogUser.grantDdl
					case RolePrivilege.DCL => catalog.catalogUser.grantDcl
				}.forall(_ == true),
				"Permission denied."
			)

		case GrantPrivilegeToGroup(privileges, _) =>

			require(
				privileges.map {
					case RolePrivilege.ACCOUNT => catalog.catalogUser.grantAccount
					case RolePrivilege.DDL => catalog.catalogUser.grantDdl
					case RolePrivilege.DCL => catalog.catalogUser.grantDcl
				}.forall(_ == true),
				"Permission denied."
			)

		case RevokePrivilegeFromUser(privileges, _) =>

			require(
				privileges.map {
					case RolePrivilege.ACCOUNT => catalog.catalogUser.grantAccount
					case RolePrivilege.DDL => catalog.catalogUser.grantDdl
					case RolePrivilege.DCL => catalog.catalogUser.grantDcl
				}.forall(_ == true),
				"Permission denied."
			)

		case RevokePrivilegeFromGroup(privileges, _) =>

			require(
				privileges.map {
					case RolePrivilege.ACCOUNT => catalog.catalogUser.grantAccount
					case RolePrivilege.DDL => catalog.catalogUser.grantDdl
					case RolePrivilege.DCL => catalog.catalogUser.grantDcl
				}.forall(_ == true),
				"Permission denied."
			)

		case _ =>
	}

}
