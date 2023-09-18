package exp

type sqlWindowFunctionExpression struct {
	fn         SQLFunctionExpression
	windowName IdentifierExpression
	window     WindowExpression
}

func NewSQLWindowFunctionExpression(
	fn SQLFunctionExpression,
	windowName IdentifierExpression,
	window WindowExpression) SQLWindowFunctionExpression {
	return sqlWindowFunctionExpression{
		fn:         fn,
		windowName: windowName,
		window:     window,
	}
}

func (swfe sqlWindowFunctionExpression) clone() sqlWindowFunctionExpression {
	return sqlWindowFunctionExpression{
		fn:         swfe.fn.Clone().(SQLFunctionExpression),
		windowName: swfe.windowName,
		window:     swfe.window,
	}
}

func (swfe sqlWindowFunctionExpression) Clone() Expression {
	return swfe.clone()
}

func (swfe sqlWindowFunctionExpression) Expression() Expression {
	return swfe
}

func (swfe sqlWindowFunctionExpression) As(val any) AliasedExpression {
	return NewAliasExpression(swfe, val)
}
func (swfe sqlWindowFunctionExpression) Eq(val any) BooleanExpression  { return eq(swfe, val) }
func (swfe sqlWindowFunctionExpression) Neq(val any) BooleanExpression { return neq(swfe, val) }
func (swfe sqlWindowFunctionExpression) Gt(val any) BooleanExpression  { return gt(swfe, val) }
func (swfe sqlWindowFunctionExpression) Gte(val any) BooleanExpression { return gte(swfe, val) }
func (swfe sqlWindowFunctionExpression) Lt(val any) BooleanExpression  { return lt(swfe, val) }
func (swfe sqlWindowFunctionExpression) Lte(val any) BooleanExpression { return lte(swfe, val) }
func (swfe sqlWindowFunctionExpression) Between(val RangeVal) RangeExpression {
	return between(swfe, val)
}

func (swfe sqlWindowFunctionExpression) NotBetween(val RangeVal) RangeExpression {
	return notBetween(swfe, val)
}

func (swfe sqlWindowFunctionExpression) Like(val any) BooleanExpression {
	return like(swfe, val)
}

func (swfe sqlWindowFunctionExpression) NotLike(val any) BooleanExpression {
	return notLike(swfe, val)
}

func (swfe sqlWindowFunctionExpression) ILike(val any) BooleanExpression {
	return iLike(swfe, val)
}

func (swfe sqlWindowFunctionExpression) NotILike(val any) BooleanExpression {
	return notILike(swfe, val)
}

func (swfe sqlWindowFunctionExpression) RegexpLike(val any) BooleanExpression {
	return regexpLike(swfe, val)
}

func (swfe sqlWindowFunctionExpression) RegexpNotLike(val any) BooleanExpression {
	return regexpNotLike(swfe, val)
}

func (swfe sqlWindowFunctionExpression) RegexpILike(val any) BooleanExpression {
	return regexpILike(swfe, val)
}

func (swfe sqlWindowFunctionExpression) RegexpNotILike(val any) BooleanExpression {
	return regexpNotILike(swfe, val)
}

func (swfe sqlWindowFunctionExpression) In(vals ...any) BooleanExpression {
	return in(swfe, vals...)
}

func (swfe sqlWindowFunctionExpression) NotIn(vals ...any) BooleanExpression {
	return notIn(swfe, vals...)
}
func (swfe sqlWindowFunctionExpression) Is(val any) BooleanExpression { return is(swfe, val) }
func (swfe sqlWindowFunctionExpression) IsNot(val any) BooleanExpression {
	return isNot(swfe, val)
}
func (swfe sqlWindowFunctionExpression) IsNull() BooleanExpression     { return is(swfe, nil) }
func (swfe sqlWindowFunctionExpression) IsNotNull() BooleanExpression  { return isNot(swfe, nil) }
func (swfe sqlWindowFunctionExpression) IsTrue() BooleanExpression     { return is(swfe, true) }
func (swfe sqlWindowFunctionExpression) IsNotTrue() BooleanExpression  { return isNot(swfe, true) }
func (swfe sqlWindowFunctionExpression) IsFalse() BooleanExpression    { return is(swfe, false) }
func (swfe sqlWindowFunctionExpression) IsNotFalse() BooleanExpression { return isNot(swfe, false) }

func (swfe sqlWindowFunctionExpression) Asc() OrderedExpression  { return asc(swfe) }
func (swfe sqlWindowFunctionExpression) Desc() OrderedExpression { return desc(swfe) }

func (swfe sqlWindowFunctionExpression) Func() SQLFunctionExpression {
	return swfe.fn
}

func (swfe sqlWindowFunctionExpression) Window() WindowExpression {
	return swfe.window
}

func (swfe sqlWindowFunctionExpression) WindowName() IdentifierExpression {
	return swfe.windowName
}

func (swfe sqlWindowFunctionExpression) HasWindow() bool {
	return swfe.window != nil
}

func (swfe sqlWindowFunctionExpression) HasWindowName() bool {
	return swfe.windowName != nil
}
