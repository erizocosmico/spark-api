package tech.sourced.api.util

import org.apache.spark.sql.catalyst.expressions._

object Expression {
  def compile(e: Expression): CompiledExpression = e match {
    case Equality(attr: AttributeReference, Literal(value, _)) => EqualExpr(new Attr(attr.toAttribute), value)
    case IsNull(attr: AttributeReference) => EqualExpr(new Attr(attr.toAttribute), null)
    case IsNotNull(attr: AttributeReference) => NotExpr(EqualExpr(new Attr(attr.toAttribute), null))
    case Not(expr) => NotExpr(compile(expr))
    case In(attr: AttributeReference, values)
      if values.forall(_.isInstanceOf[Literal]) =>
      InExpr(new Attr(attr.toAttribute), values.map({ case Literal(value, _) => value }))
    case And(l, r) => AndExpr(compile(l), compile(r))
    case Or(l, r) => OrExpr(compile(l), compile(r))
    case _ => UnhandledExpr()
  }
}

sealed trait CompiledExpression {

  def eval(cols: Map[String, Any]): Option[Boolean]

  def matchingCases: Map[String, Seq[Any]] =
    getMatchingCases.groupBy(_._1).mapValues(_.map(_._2))

  protected def getMatchingCases: Array[(String, Any)] = this match {
    case EqualExpr(attr, value) => Array((attr.name, value))
    case BinaryExpr(left, right) => left.getMatchingCases ++ right.getMatchingCases
    case _ => Array()
  }

  def expressions: Seq[CompiledExpression] = Seq(this)

  def sources: Seq[String]

}

case class Attr(name: String, source: String) {

  def this(attr: Attribute) = this(attr.name, if (attr.metadata.contains("source")) {
    attr.metadata.getString("source")
  } else {
    ""
  })

}

case class InExpr(attr: Attr, vals: Seq[Any]) extends CompiledExpression {

  override def toString: String = s"${attr.name} IN (${vals.mkString(", ")})"

  def eval(cols: Map[String, Any]): Option[Boolean] = cols.get(attr.name).map(vals.contains)

  def sources: Seq[String] = Seq(attr.source)

}

class BinaryExpr(val left: CompiledExpression, val right: CompiledExpression) extends CompiledExpression {

  // Expected to be overridden
  def action(l: Option[Boolean], r: Option[Boolean]): Option[Boolean] = None

  def eval(cols: Map[String, Any]): Option[Boolean] = action(left.eval(cols), right.eval(cols))

  def sources: Seq[String] = left.sources ++ right.sources

}

object BinaryExpr {

  def unapply(e: BinaryExpr): Option[(CompiledExpression, CompiledExpression)] = Some((e.left, e.right))

}

case class EqualExpr(attr: Attr, value: Any) extends CompiledExpression {

  override def toString: String = s"${attr.name} = '$value'"

  def eval(cols: Map[String, Any]): Option[Boolean] = cols.get(attr.name).map(_ == value)

  def sources: Seq[String] = Seq(attr.source)

}

case class NotExpr(e: CompiledExpression) extends CompiledExpression {

  override def toString: String = s"NOT (${e.toString})"

  def eval(cols: Map[String, Any]): Option[Boolean] = e.eval(cols).map(!_)

  def sources: Seq[String] = e.sources

}

case class AndExpr(l: CompiledExpression, r: CompiledExpression) extends BinaryExpr(l, r) {

  override def toString: String = s"(${l.toString} AND ${r.toString})"

  override def action(l: Option[Boolean], r: Option[Boolean]): Option[Boolean] = {
    Seq(l, r) match {
      case seq if seq.flatten.isEmpty => None
      case seq => seq.map({
        case None => false
        case Some(v) => v
      }).reduceLeftOption((lb, rb) => lb && rb)
    }
  }

  override def expressions: Seq[CompiledExpression] = Seq(l, r)

}

case class OrExpr(l: CompiledExpression, r: CompiledExpression) extends BinaryExpr(l, r) {

  override def toString: String = s"(${l.toString} OR ${r.toString})"

  override def action(l: Option[Boolean], r: Option[Boolean]): Option[Boolean] =
    Seq(l, r).flatten.reduceLeftOption((lb, rb) => lb || rb)

}

case class UnhandledExpr() extends CompiledExpression {

  override def eval(cols: Map[String, Any]): Option[Boolean] = None

  def sources: Seq[String] = Seq()

}
