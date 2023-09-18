package util

import (
	"reflect"
	"sort"
	"strings"

	"github.com/Tooooommy/builder/v9/internal/tag"
)

type (
	ColumnData struct {
		ColumnName     string
		FieldIndex     []int
		ShouldInsert   bool
		ShouldUpdate   bool
		DefaultIfEmpty bool
		GoType         reflect.Type
	}
	ColumnMap map[string]ColumnData
)

func newColumnMap(t reflect.Type, fieldIndex []int, prefixes []string) ColumnMap {
	cm, n := ColumnMap{}, t.NumField()
	var subColMaps []ColumnMap
	for i := 0; i < n; i++ {
		f := t.Field(i)
		if f.Anonymous && (f.Type.Kind() == reflect.Struct || f.Type.Kind() == reflect.Ptr) {
			builderTag := tag.New("db", f.Tag)
			if !builderTag.Contains("-") {
				subColMaps = append(subColMaps, getStructColumnMap(&f, fieldIndex, builderTag.Values(), prefixes))
			}
		} else if f.PkgPath == "" {
			dbTag := tag.New("db", f.Tag)
			// if PkgPath is empty then it is an exported field
			columnName := getColumnName(&f, dbTag)
			if !shouldIgnoreField(dbTag) {
				if !implementsScanner(f.Type) {
					subCm := getStructColumnMap(&f, fieldIndex, []string{columnName}, prefixes)
					if len(subCm) != 0 {
						subColMaps = append(subColMaps, subCm)
						continue
					}
				}
				builderTag := tag.New("builder", f.Tag)
				columnName = strings.Join(append(prefixes, columnName), ".")
				cm[columnName] = newColumnData(&f, columnName, fieldIndex, builderTag)
			}
		}
	}
	return cm.Merge(subColMaps)
}

func (cm ColumnMap) Cols() []string {
	structCols := make([]string, 0, len(cm))
	for key := range cm {
		structCols = append(structCols, key)
	}
	sort.Strings(structCols)
	return structCols
}

func (cm ColumnMap) Merge(colMaps []ColumnMap) ColumnMap {
	for _, subCm := range colMaps {
		for key, val := range subCm {
			if _, ok := cm[key]; !ok {
				cm[key] = val
			}
		}
	}
	return cm
}

func implementsScanner(t reflect.Type) bool {
	if IsPointer(t.Kind()) {
		t = t.Elem()
	}
	if reflect.PtrTo(t).Implements(scannerType) {
		return true
	}
	if !IsStruct(t.Kind()) {
		return true
	}

	return false
}

func newColumnData(f *reflect.StructField, columnName string, fieldIndex []int, builderTag tag.Options) ColumnData {
	return ColumnData{
		ColumnName:     columnName,
		ShouldInsert:   !builderTag.Contains(skipInsertTagName),
		ShouldUpdate:   !builderTag.Contains(skipUpdateTagName),
		DefaultIfEmpty: builderTag.Contains(defaultIfEmptyTagName),
		FieldIndex:     concatFieldIndexes(fieldIndex, f.Index),
		GoType:         f.Type,
	}
}

func getStructColumnMap(f *reflect.StructField, fieldIndex []int, fieldNames, prefixes []string) ColumnMap {
	subFieldIndexes := concatFieldIndexes(fieldIndex, f.Index)
	subPrefixes := append(prefixes, fieldNames...)
	if f.Type.Kind() == reflect.Ptr {
		return newColumnMap(f.Type.Elem(), subFieldIndexes, subPrefixes)
	}
	return newColumnMap(f.Type, subFieldIndexes, subPrefixes)
}

func getColumnName(f *reflect.StructField, dbTag tag.Options) string {
	if dbTag.IsEmpty() {
		return columnRenameFunction(f.Name)
	}
	return dbTag.Values()[0]
}

func shouldIgnoreField(dbTag tag.Options) bool {
	if dbTag.Equals("-") {
		return true
	} else if dbTag.IsEmpty() && ignoreUntaggedFields {
		return true
	}

	return false
}

// safely concat two fieldIndex slices into one.
func concatFieldIndexes(fieldIndexPath, fieldIndex []int) []int {
	fieldIndexes := make([]int, 0, len(fieldIndexPath)+len(fieldIndex))
	fieldIndexes = append(fieldIndexes, fieldIndexPath...)
	return append(fieldIndexes, fieldIndex...)
}
