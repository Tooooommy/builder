package exp

type (
	literal struct {
		literal string
		args    []any
	}
)

// Creates a new SQL literal with the provided arguments.
//   L("a = 1") -> a = 1
// You can also you placeholders. All placeholders within a Literal are represented by '?'
//   L("a = ?", "b") -> a = 'b'
// Literals can also contain placeholders for other expressions
//   L("(? AND ?) OR (?)", I("a").Eq(1), I("b").Eq("b"), I("c").In([]string{"a", "b", "c"}))
func NewLiteralExpression(sql string, args ...any) LiteralExpression {
	return literal{literal: sql, args: args}
}

// Returns a literal for the '*' operator
func Star() LiteralExpression {
	return NewLiteralExpression("*")
}

// Returns a literal for the 'DEFAULT'
func Default() LiteralExpression {
	return NewLiteralExpression("DEFAULT")
}

func (l literal) Clone() Expression {
	return NewLiteralExpression(l.literal, l.args...)
}

func (l literal) Literal() string {
	return l.literal
}

func (l literal) Args() []any {
	return l.args
}

func (l literal) Expression() Expression                   { return l }
func (l literal) As(val any) AliasedExpression             { return NewAliasExpression(l, val) }
func (l literal) Eq(val any) BooleanExpression             { return eq(l, val) }
func (l literal) Neq(val any) BooleanExpression            { return neq(l, val) }
func (l literal) Gt(val any) BooleanExpression             { return gt(l, val) }
func (l literal) Gte(val any) BooleanExpression            { return gte(l, val) }
func (l literal) Lt(val any) BooleanExpression             { return lt(l, val) }
func (l literal) Lte(val any) BooleanExpression            { return lte(l, val) }
func (l literal) Asc() OrderedExpression                   { return asc(l) }
func (l literal) Desc() OrderedExpression                  { return desc(l) }
func (l literal) Between(val RangeVal) RangeExpression     { return between(l, val) }
func (l literal) NotBetween(val RangeVal) RangeExpression  { return notBetween(l, val) }
func (l literal) Like(val any) BooleanExpression           { return like(l, val) }
func (l literal) NotLike(val any) BooleanExpression        { return notLike(l, val) }
func (l literal) ILike(val any) BooleanExpression          { return iLike(l, val) }
func (l literal) NotILike(val any) BooleanExpression       { return notILike(l, val) }
func (l literal) RegexpLike(val any) BooleanExpression     { return regexpLike(l, val) }
func (l literal) RegexpNotLike(val any) BooleanExpression  { return regexpNotLike(l, val) }
func (l literal) RegexpILike(val any) BooleanExpression    { return regexpILike(l, val) }
func (l literal) RegexpNotILike(val any) BooleanExpression { return regexpNotILike(l, val) }
func (l literal) In(vals ...any) BooleanExpression         { return in(l, vals...) }
func (l literal) NotIn(vals ...any) BooleanExpression      { return notIn(l, vals...) }
func (l literal) Is(val any) BooleanExpression             { return is(l, val) }
func (l literal) IsNot(val any) BooleanExpression          { return isNot(l, val) }
func (l literal) IsNull() BooleanExpression                { return is(l, nil) }
func (l literal) IsNotNull() BooleanExpression             { return isNot(l, nil) }
func (l literal) IsTrue() BooleanExpression                { return is(l, true) }
func (l literal) IsNotTrue() BooleanExpression             { return isNot(l, true) }
func (l literal) IsFalse() BooleanExpression               { return is(l, false) }
func (l literal) IsNotFalse() BooleanExpression            { return isNot(l, false) }

func (l literal) BitwiseInversion() BitwiseExpression        { return bitwiseInversion(l) }
func (l literal) BitwiseOr(val any) BitwiseExpression        { return bitwiseOr(l, val) }
func (l literal) BitwiseAnd(val any) BitwiseExpression       { return bitwiseAnd(l, val) }
func (l literal) BitwiseXor(val any) BitwiseExpression       { return bitwiseXor(l, val) }
func (l literal) BitwiseLeftShift(val any) BitwiseExpression { return bitwiseLeftShift(l, val) }
func (l literal) BitwiseRightShift(val any) BitwiseExpression {
	return bitwiseRightShift(l, val)
}
