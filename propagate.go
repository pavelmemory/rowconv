package main

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	DbColumn = "db_column"
)

type Mapper func(dst interface{}, rows *sql.Rows) error

var (
	holderElementTypes    = map[reflect.Type]Mapper{}
	holderElementTypesMtx sync.RWMutex
)

var (
	strictColumnTypeCheck   bool
	strictColumnAmountCheck bool
)

// SetStrictColumnTypeCheck configures mapper to check types of struct fields with types returned by database driver
// if types are different and 'strict' set to 'true' the error will be produced
func SetStrictColumnTypeCheck(strict bool) {
	strictColumnTypeCheck = strict
}

// SetStrictColumnAmountCheck configures mapper to check amount of struct fields to be exact to amount of columns returned
// if amount is different and 'strict' set to 'true' the error will be produced
func SetStrictColumnAmountCheck(strict bool) {
	strictColumnAmountCheck = strict
}

var (
	smallestStructDecompositions map[reflect.Type]struct{} = func() map[reflect.Type]struct{} {
		return map[reflect.Type]struct{}{
			reflect.TypeOf(time.Time{}):     struct{}{},
			reflect.TypeOf(time.Location{}): struct{}{},
		}
	}()
	smallestStructDecompositionsMtx sync.RWMutex
)

// SmallestStructDecomposition adds struct to set of structs that not need to be field-initialized,
// such as time.Time and time.Location
func SmallestStructDecomposition(t reflect.Type) {
	smallestStructDecompositionsMtx.Lock()
	smallestStructDecompositions[t] = struct{}{}
	smallestStructDecompositionsMtx.Unlock()
}

// Propagate converts rows into structs/basic values according to settings and put them into dst
func Propagate(dst interface{}, rows *sql.Rows) error {
	holderType := reflect.TypeOf(dst)
	if holderType.Kind() != reflect.Ptr || holderType.Elem().Kind() != reflect.Slice {
		return errors.New("destination must be a pointer to the slice")
	}
	holderElementType, err := elementType(holderType.Elem())
	if err != nil {
		return err
	}
	holderElementTypesMtx.RLock()
	mapper := holderElementTypes[holderElementType]
	holderElementTypesMtx.RUnlock()
	if mapper == nil {
		holderElementTypesMtx.Lock()
		mapper = holderElementTypes[holderElementType]
		if mapper == nil {
			columnTypes, err := rows.ColumnTypes()
			if err != nil {
				holderElementTypesMtx.Unlock()
				return err
			}
			mapper, err = createMappers(holderElementType, columnTypes)
			if err != nil {
				holderElementTypesMtx.Unlock()
				return err
			}
			holderElementTypes[holderElementType] = mapper
			holderElementTypesMtx.Unlock()
		}
	}
	return mapper(dst, rows)
}

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

func isScanSupported(t reflect.Type) bool {
	return t.Implements(scannerType)
}

func isSmallestStructDecomposition(t reflect.Type) bool {
	smallestStructDecompositionsMtx.RLock()
	_, smallest := smallestStructDecompositions[t]
	smallestStructDecompositionsMtx.RUnlock()
	if !smallest {
		smallest = isScanSupported(t)
	}
	return smallest
}

func elementType(dstType reflect.Type) (reflect.Type, error) {
	inspection := dstType
	for {
		switch inspection.Kind() {
		case reflect.Map:
			inspection = inspection.Elem()
		case reflect.Slice:
			if inspection.Elem().Kind() == reflect.Uint8 {
				return inspection, nil
			}
			inspection = inspection.Elem()
		case reflect.Chan, reflect.Func, reflect.Invalid, reflect.Interface, reflect.UnsafePointer, reflect.Array:
			return nil, errors.New("unsupported type: " + dstType.String())
		default:
			return inspection, nil
		}
	}
}

type fieldAccessor struct {
	fieldType  reflect.Type
	fieldIndex []int
}

func getFieldAccessorsRecursively(columnAliasToAccessor map[string]fieldAccessor, folding []int, inspectionType reflect.Type) error {
	for {
		switch inspectionType.Kind() {
		case reflect.Ptr:
			inspectionType = inspectionType.Elem()

		case reflect.Struct:
			fields := inspectionType.NumField()
			for i := 0; i < fields; i++ {
				field := inspectionType.Field(i)
				fieldKind := field.Type.Kind()
				if fieldKind == reflect.Struct || // is struct or pointer to struct
					fieldKind == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct {
					if err := getFieldAccessorsRecursively(columnAliasToAccessor, append(folding, i), field.Type); err != nil {
						return err
					}
				}

				columnAlias, found := field.Tag.Lookup(DbColumn)
				if !found {
					columnAlias = strings.ToLower(field.Name)
				}
				columnAliasToAccessor[columnAlias] = fieldAccessor{
					fieldType:  field.Type,
					fieldIndex: append(folding, i),
				}
			}
			return nil
		}
	}
	return errors.New("not supported type: " + inspectionType.String())
}

func getFieldAccessors(dstType reflect.Type) (map[string]fieldAccessor, error) {
	columnAliasToAccessor := map[string]fieldAccessor{}
	if err := getFieldAccessorsRecursively(columnAliasToAccessor, nil, dstType); err != nil {
		return nil, err
	}
	return columnAliasToAccessor, nil
}

type structProvider func() (reflect.Value, error)

var (
	structProviders    = map[reflect.Type]structProvider{}
	structProvidersMtx sync.RWMutex
)

func getStructProviderSync(forType reflect.Type) (provider structProvider, err error) {
	structProvidersMtx.RLock()
	provider, found := structProviders[forType]
	if found {
		structProvidersMtx.RUnlock()
		return
	}
	structProvidersMtx.RUnlock()
	structProvidersMtx.Lock()
	provider, err = getStructProvider(forType)
	structProvidersMtx.Unlock()
	return
}

func getStructProvider(forType reflect.Type) (structProvider, error) {
	provider, found := structProviders[forType]
	if found {
		return provider, nil
	}

	actualType, ptrDepth, err := unwrapPtrStructType(forType)
	if err != nil {
		return nil, err
	}

	var initActions []func(reflect.Value) error
	actualValue := reflect.New(actualType).Elem()
	for i := 0; i < actualValue.NumField(); i++ {
		actualValueField := actualValue.Field(i)
	fieldDetermine:
		for ptrNesting := 0; true; ptrNesting++ {
			actualValueFieldType := actualValueField.Type()
			switch actualValueField.Kind() {
			case reflect.Struct:
				if isSmallestStructDecomposition(actualValueFieldType) {
					break fieldDetermine
				}
				provider, err := getStructProvider(actualValueFieldType)
				if err != nil {
					return nil, err
				}
				idx := i

				initActions = append(initActions, func(initStruct reflect.Value) error {
					initFieldValue, err := provider()
					if err != nil {
						return err
					}
					for ptrDepth := ptrNesting; ptrDepth > 0; ptrDepth-- {
						initFieldValue = initFieldValue.Addr()
					}
					initStruct.Field(idx).Set(initFieldValue)
					return nil
				})

			case reflect.Ptr:
				// create pointer before accessing its type information
				actualValueField = reflect.New(actualValueFieldType.Elem()).Elem()
				continue fieldDetermine

			default:
				// field of base type/reference or alias to base type/reference that not need to be initialized
			}
			break fieldDetermine
		}
	}

	provider = func() (reflect.Value, error) {
		holderValue := reflect.New(actualType).Elem()
		for _, initAction := range initActions {
			if err := initAction(holderValue); err != nil {
				return reflect.Value{}, err
			}
		}
		for ptrNesting := ptrDepth; ptrNesting > 0; ptrNesting-- {
			holderValue = holderValue.Addr()
		}
		return holderValue, nil
	}
	structProviders[forType] = provider
	return provider, nil
}

// this func creates new struct by traversing it's structure with reflection
// and initializes struct fields
//func newStructRecursively(dst reflect.Type) (reflect.Value, error) {
//	dstActual, wrapLevels, err := unwrapPtrStructType(dst)
//	if err != nil {
//		return reflect.Value{}, err
//	}
//
//	holderValue := reflect.New(dstActual).Elem()
//	if err := initFieldsRecursively(holderValue); err != nil {
//		return reflect.Value{}, err
//	}
//	for i := wrapLevels; i > 0; i-- {
//		holderValue = holderValue.Addr()
//	}
//	return holderValue, nil
//}
//
//func initFieldsRecursively(holderValue reflect.Value) error {
//	for i := 0; i < holderValue.NumField(); i++ {
//		holderField := holderValue.Field(i)
//		switch holderField.Kind() {
//		case  reflect.Struct:
//			if err := initFieldsRecursively(holderField); err != nil {
//				return err
//			}
//		case reflect.Ptr:
//			// create pointer before accessing its type information
//			holderFieldPtr := reflect.New(holderField.Type().Elem())
//			holderFieldElem := holderFieldPtr.Elem()
//			if holderFieldElem.Kind() == reflect.Struct {
//				if isSmallestStructDecomposition(holderFieldElem.Type()) {
//					continue
//				}
//				holderFieldValue, err := newStructRecursively(holderField.Type())
//				if err != nil {
//					return err
//				}
//				holderField.Set(holderFieldValue)
//			}
//		default:
//		// field of base type/reference or alias to base type/reference that not need to be initialized
//		}
//	}
//	return nil
//}

func unwrapPtrStructType(wrapped reflect.Type) (reflect.Type, int, error) {
	actualType := wrapped
	levels := 0
	for {
		switch actualType.Kind() {
		case reflect.Ptr:
			levels++
			actualType = actualType.Elem()
		case reflect.Struct:
			return actualType, levels, nil
		default:
			return nil, 0, errors.New("underline type is not a struct: " + actualType.Kind().String())
		}
	}
}

func unwrapPtrStructValue(wrapped reflect.Value) (reflect.Value, int, error) {
	actualValue := wrapped
	levels := 0
	for {
		switch actualValue.Kind() {
		case reflect.Ptr:
			levels++
			actualValue = actualValue.Elem()
		case reflect.Struct:
			return actualValue, levels, nil
		default:
			return reflect.Value{}, 0, errors.New("underline type is not a struct: " + actualValue.Kind().String())
		}
	}
}

func isSingleBasicType(dstType reflect.Type) bool {
	for {
		switch dstType.Kind() {
		case reflect.Ptr:
			dstType = dstType.Elem()
		case
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.String,
			reflect.Float32, reflect.Float64:
			return true
		case reflect.Slice:
			return dstType.Elem().Kind() == reflect.Uint8
		case reflect.Struct:
			return isSmallestStructDecomposition(dstType)
		default:
			return false
		}
	}
}

func singleColumnMapper(forType reflect.Type) func(dst interface{}, rows *sql.Rows) error {
	return func(holder interface{}, rows *sql.Rows) error {
		for rows.Next() {
			holderElement := reflect.New(forType)
			err := rows.Scan(holderElement.Interface())
			if err != nil {
				return err
			}
			err = putToHolder(holder, holderElement.Elem())
			if err != nil {
				rows.Close()
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	}
}

func getHolderSuppliers(dstType reflect.Type, columnTypes []*sql.ColumnType) (holderSuppliers []holderSupplier, err error) {
	columnAliasToAccessor, err := getFieldAccessors(dstType)
	if err != nil {
		return nil, err
	}

	for _, columnType := range columnTypes {
		accessor, found := columnAliasToAccessor[strings.ToLower(columnType.Name())]
		if found {
			if strictColumnTypeCheck && columnType.ScanType() != accessor.fieldType {
				return nil, fmt.Errorf("value for column/alias: %v can't be stored into the type: %v; required type: %v", columnType.Name(), accessor.fieldType, columnType.ScanType())
			}
			holderSuppliers = append(holderSuppliers, holderByFieldIndexPath(accessor.fieldIndex))
		} else {
			if strictColumnAmountCheck {
				return nil, errors.New("no mapping exists for column/alias: " + columnType.Name())
			}
			holderSuppliers = append(holderSuppliers, holderSkipColumn)
		}
	}
	return
}

func createMappers(holderElementType reflect.Type, columnTypes []*sql.ColumnType) (Mapper, error) {
	if isSingleBasicType(holderElementType) {
		return singleColumnMapper(holderElementType), nil
	}

	holderSuppliers, err := getHolderSuppliers(holderElementType, columnTypes)
	if err != nil {
		return nil, err
	}

	return func(holder interface{}, rows *sql.Rows) error {
		for rows.Next() {
			// this call creates initialized struct by traversing throw its structure with reflection
			//dstValue, err := newStructRecursively(dstType)
			provider, err := getStructProviderSync(holderElementType)
			if err != nil {
				rows.Close()
				return err
			}
			holderElement, err := provider()
			if err != nil {
				rows.Close()
				return err
			}

			underlyingValue, _, err := unwrapPtrStructValue(holderElement)
			if err != nil {
				return err
			}
			holderElementFields := make([]interface{}, len(holderSuppliers))
			for i, holderSupplier := range holderSuppliers {
				holderElementFields[i] = holderSupplier(underlyingValue)
			}
			if err = rows.Scan(holderElementFields...); err != nil {
				return err
			}

			if err = putToHolder(holder, holderElement); err != nil {
				rows.Close()
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	}, nil
}

type holderSupplier func(underlyingValue reflect.Value) interface{}

func holderByFieldIndexPath(holderIndexPath []int) holderSupplier {
	return func(underlyingValue reflect.Value) interface{} {
		return underlyingValue.FieldByIndex(holderIndexPath).Addr().Interface()
	}
}

func holderSkipColumn(underlyingValue reflect.Value) (skip interface{}) { return &skip }

func putToHolder(holder interface{}, value reflect.Value) error {
	dstHolderType := reflect.TypeOf(holder)
	dstHolderValue := reflect.ValueOf(holder)
	for {
		switch dstHolderType.Kind() {
		case reflect.Ptr:
			dstHolderType = dstHolderType.Elem()
			dstHolderValue = dstHolderValue.Elem()
		case reflect.Slice:
			newSlice := reflect.Append(dstHolderValue, value)
			dstHolderValue.Set(newSlice)
			return nil

		//case reflect.Map:
		//	return errors.New("not implemented: holder for map")
		default:
			return errors.New("not implemented: holder for type: " + dstHolderType.Name())
		}
	}
}