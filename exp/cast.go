package exp

type cast struct {
	casted Expression
	t      LiteralExpression
}

// Creates a new Casted expression
//  Cast(I("a"), "NUMERIC") -> CAST("a" AS NUMERIC)
func NewCastExpression(e Expression, t string) CastExpression {
	return cast{casted: e, t: NewLiteralExpression(t)}
}

func (c cast) Casted() Expression {
	return c.casted
}

func (c cast) Type() LiteralExpression {
	return c.t
}

func (c cast) Clone() Expression {
	return cast{casted: c.casted.Clone(), t: c.t}
}

func (c cast) Expression() Expression                   { return c }
func (c cast) As(val any) AliasedExpression             { return NewAliasExpression(c, val) }
func (c cast) Eq(val any) BooleanExpression             { return eq(c, val) }
func (c cast) Neq(val any) BooleanExpression            { return neq(c, val) }
func (c cast) Gt(val any) BooleanExpression             { return gt(c, val) }
func (c cast) Gte(val any) BooleanExpression            { return gte(c, val) }
func (c cast) Lt(val any) BooleanExpression             { return lt(c, val) }
func (c cast) Lte(val any) BooleanExpression            { return lte(c, val) }
func (c cast) Asc() OrderedExpression                   { return asc(c) }
func (c cast) Desc() OrderedExpression                  { return desc(c) }
func (c cast) Like(i any) BooleanExpression             { return like(c, i) }
func (c cast) NotLike(i any) BooleanExpression          { return notLike(c, i) }
func (c cast) ILike(i any) BooleanExpression            { return iLike(c, i) }
func (c cast) NotILike(i any) BooleanExpression         { return notILike(c, i) }
func (c cast) RegexpLike(val any) BooleanExpression     { return regexpLike(c, val) }
func (c cast) RegexpNotLike(val any) BooleanExpression  { return regexpNotLike(c, val) }
func (c cast) RegexpILike(val any) BooleanExpression    { return regexpILike(c, val) }
func (c cast) RegexpNotILike(val any) BooleanExpression { return regexpNotILike(c, val) }
func (c cast) In(i ...any) BooleanExpression            { return in(c, i...) }
func (c cast) NotIn(i ...any) BooleanExpression         { return notIn(c, i...) }
func (c cast) Is(i any) BooleanExpression               { return is(c, i) }
func (c cast) IsNot(i any) BooleanExpression            { return isNot(c, i) }
func (c cast) IsNull() BooleanExpression                { return is(c, nil) }
func (c cast) IsNotNull() BooleanExpression             { return isNot(c, nil) }
func (c cast) IsTrue() BooleanExpression                { return is(c, true) }
func (c cast) IsNotTrue() BooleanExpression             { return isNot(c, true) }
func (c cast) IsFalse() BooleanExpression               { return is(c, false) }
func (c cast) IsNotFalse() BooleanExpression            { return isNot(c, false) }
func (c cast) Distinct() SQLFunctionExpression          { return NewSQLFunctionExpression("DISTINCT", c) }
func (c cast) Between(val RangeVal) RangeExpression     { return between(c, val) }
func (c cast) NotBetween(val RangeVal) RangeExpression  { return notBetween(c, val) }
