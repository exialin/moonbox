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

package moonbox.core.datasys

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.types.StructType

// 这里self是什么意思？
trait Pushdownable { self: DataSystem =>
	val supportedOperators: Seq[Class[_]]
	val supportedJoinTypes: Seq[JoinType]
	val supportedExpressions: Seq[Class[_]]
	val beGoodAtOperators: Seq[Class[_]]
	val supportedUDF: Seq[String]

	def isGoodAt(operator: Class[_]): Boolean = {
		beGoodAtOperators.contains(operator)
	}

	// 只有SparkDataSystem是true，其他都是false
	def isSupportAll: Boolean

	def isSupport(plan: LogicalPlan): Boolean = {
		isSupportAll || {(plan match {
			case join: Join =>
				supportedOperators.contains(plan.getClass) && supportedJoinTypes.contains(join.joinType)
			case _ =>
				supportedOperators.contains(plan.getClass)
		}) && allExpressionSupport(plan)
		}
	}

	// 遍历逻辑计划的所有表达式，若全部支持，则返回true
	private def allExpressionSupport(plan: LogicalPlan): Boolean = {
		def traverseExpression(expression: Expression): Boolean ={
			expression match {
				case udf: ScalaUDF =>
					udf.udfName match {
						case Some(udfName) =>
							supportedExpressions.contains(udf.getClass) && supportedUDF.contains(udfName)
						case None => false
					}
				case udaf: ScalaUDAF => false
				case expr => supportedExpressions.contains(expr.getClass) && expression.children.forall(traverseExpression)
			}
		}
		plan.expressions.forall { expr =>
			traverseExpression(expr)
		}
	}

	def fastEquals(other: DataSystem): Boolean

	// 在Pushdown.replacePushdownSubtree方法中被调用，针对部分下推的情况，这时需要从数据源拉取数据作为Spark
	// 的DataFrame，而且会进行计划树的替换
	def buildScan(plan: LogicalPlan, sparkSession: SparkSession): DataFrame

	// 在SparkEngine.sql中被调用，针对可以完全下推到数据源的情况，返回类型是DataTable
	def buildQuery(plan: LogicalPlan, sparkSession: SparkSession): DataTable
}

trait Insertable {
	def insert(table: DataTable, saveMode: SaveMode): Unit
}

trait Truncatable {
    def truncate(): Unit
}

trait Deletable {
	def delete(key: Any, condition: Expression): Unit
}

trait Updatable {
	def update(data: DataFrame,
		tableSchema: Option[StructType],
		isCaseSensitive: Boolean,
		parameter: Map[String, String]): Unit
}
