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

var (
	strictColumnTypeCheck   bool
	strictColumnAmountCheck bool
	strictConfigsMtx        sync.RWMutex

	elementScanDefinitions = scanDefinitions{}
	holderElementTypesMtx  sync.RWMutex

	smallestStructDecompositions = map[reflect.Type]struct{}{
		reflect.TypeOf(time.Time{}):     {},
		reflect.TypeOf(time.Location{}): {},
	}
	smallestStructDecompositionsMtx sync.RWMutex

	scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
)

// StrictColumnTypeCheck configures mapper to check types of struct fields with types returned by database driver
// if types are different and 'strict' set to 'true' the error will be produced
func StrictColumnTypeCheck(strict bool) {
	strictConfigsMtx.Lock()
	strictColumnTypeCheck = strict
	strictConfigsMtx.Unlock()
}

// StrictColumnAmountCheck configures mapper to check amount of struct fields to be exact to amount of columns returned
// if amount is different and 'strict' set to 'true' the error will be produced
func StrictColumnAmountCheck(strict bool) {
	strictConfigsMtx.Lock()
	strictColumnAmountCheck = strict
	strictConfigsMtx.Unlock()
}

// SmallestStructDecomposition adds struct to set of structs that not need to be field-initialized,
// such as time.Time and time.Location
// `time.Time` and `time.Location` are added by default
func SmallestStructDecomposition(t reflect.Type) {
	smallestStructDecompositionsMtx.Lock()
	smallestStructDecompositions[t] = struct{}{}
	smallestStructDecompositionsMtx.Unlock()
}

// Propagate converts rows into structs/basic values according to settings and put them into dst
func Propagate(dst interface{}, rows *sql.Rows) error {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	holderType := reflect.TypeOf(dst)
	if holderType.Kind() != reflect.Ptr || holderType.Elem().Kind() != reflect.Slice {
		return errors.New("destination must be a pointer to the slice")
	}
	holderElementType, err := elementType(holderType.Elem())
	if err != nil {
		return err
	}
	holderElementTypesMtx.RLock()
	scanDef, found := elementScanDefinitions.findScanDefinition(holderElementType, columnTypes)
	holderElementTypesMtx.RUnlock()
	if !found {
		holderElementTypesMtx.Lock()
		scanDef, found = elementScanDefinitions.findScanDefinition(holderElementType, columnTypes)
		if !found {
			scanDef, err = elementScanDefinitions.createScanDefinition(holderElementType, columnTypes)
			if err != nil {
				holderElementTypesMtx.Unlock()
				return err
			}
			holderElementTypesMtx.Unlock()
		}
	}
	return scanDef.mapper(dst, rows)
}

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
	LoopDetermineField:
		for ptrNesting := 0; true; ptrNesting++ {
			actualValueFieldType := actualValueField.Type()
			switch actualValueField.Kind() {
			case reflect.Struct:
				if isSmallestStructDecomposition(actualValueFieldType) {
					break LoopDetermineField
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
				continue LoopDetermineField

			default:
				// field of base type/reference or alias to base type/reference that not need to be initialized
			}
			break LoopDetermineField
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
		inject, err := prepareInjector(holder)
		if err != nil {
			rows.Close()
			return err
		}
		for rows.Next() {
			holderElement := reflect.New(forType)
			err := rows.Scan(holderElement.Interface())
			if err != nil {
				return err
			}
			inject(holderElement.Elem())
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

	strictConfigsMtx.RLock()
	camtChk := strictColumnAmountCheck
	ctChk := strictColumnTypeCheck
	strictConfigsMtx.RUnlock()

	for _, columnType := range columnTypes {
		accessor, found := columnAliasToAccessor[strings.ToLower(columnType.Name())]
		if found {
			if ctChk && columnType.ScanType() != accessor.fieldType {
				return nil, fmt.Errorf("value for column/alias: %v can't be stored into the type: %v; required type: %v", columnType.Name(), accessor.fieldType, columnType.ScanType())
			}
			holderSuppliers = append(holderSuppliers, holderByFieldIndexPath(accessor.fieldIndex))
		} else {
			if camtChk {
				return nil, errors.New("no mapping exists for column/alias: " + columnType.Name())
			}
			holderSuppliers = append(holderSuppliers, holderSkipColumn)
		}
	}
	return
}

func createRowsMapper(holderElementType reflect.Type, columnTypes []*sql.ColumnType) (rowsMapper, error) {
	if isSingleBasicType(holderElementType) {
		return singleColumnMapper(holderElementType), nil
	}

	holderSuppliers, err := getHolderSuppliers(holderElementType, columnTypes)
	if err != nil {
		return nil, err
	}

	return func(holder interface{}, rows *sql.Rows) error {
		inject, err := prepareInjector(holder)
		if err != nil {
			rows.Close()
			return err
		}
		for rows.Next() {
			// this call creates initialized struct by traversing throw its structure with reflection
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

			inject(holderElement)
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

func prepareInjector(holder interface{}) (func(value reflect.Value), error) {
	dstHolderType := reflect.TypeOf(holder)
	dstHolderValue := reflect.ValueOf(holder)
	for {
		switch dstHolderType.Kind() {
		case reflect.Ptr:
			dstHolderType = dstHolderType.Elem()
			dstHolderValue = dstHolderValue.Elem()
		case reflect.Slice:
			return func(value reflect.Value) {
				newSlice := reflect.Append(dstHolderValue, value)
				dstHolderValue.Set(newSlice)
			}, nil

			//case reflect.Map:
			//	return errors.New("not implemented: holder for map")
		default:
			return nil, errors.New("not implemented: holder for type: " + dstHolderType.Name())
		}
	}
}

type rowsMapper func(dst interface{}, rows *sql.Rows) error

type scanDefinition struct {
	columnTypes []*sql.ColumnType
	mapper      rowsMapper
}

type scanDefinitions map[reflect.Type][]scanDefinition

func (sd scanDefinitions) findScanDefinition(elementType reflect.Type, columnTypes []*sql.ColumnType) (scanDefinition, bool) {
	scanDefs, found := sd[elementType]
	if !found {
		return scanDefinition{}, false
	}
LoopScanDef:
	for _, scanDef := range scanDefs {
		if len(scanDef.columnTypes) != len(columnTypes) {
			continue
		}
		for i := 0; i < len(scanDef.columnTypes); i++ {
			if *scanDef.columnTypes[i] != *columnTypes[i] {
				continue LoopScanDef
			}
		}
		return scanDef, true
	}
	return scanDefinition{}, false
}

func (sd scanDefinitions) createScanDefinition(elementType reflect.Type, columnTypes []*sql.ColumnType) (scanDefinition, error) {
	mapper, err := createRowsMapper(elementType, columnTypes)
	if err != nil {
		return scanDefinition{}, err
	}
	scanDef := scanDefinition{mapper: mapper, columnTypes: columnTypes}
	sd[elementType] = append(sd[elementType], scanDef)
	return scanDef, nil
}
