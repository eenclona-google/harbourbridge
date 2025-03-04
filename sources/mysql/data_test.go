// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"fmt"
	"math/bits"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/logger"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func init() {
	logger.Log = zap.NewNop()
}

type spannerData struct {
	table string
	cols  []string
	vals  []interface{}
}

// Basic smoke test of ProcessDataRow. The core part of this code path
// (ConvertData) is tested in TestConvertData.
func TestProcessDataRow(t *testing.T) {
	tableName := "testtable"
	cols := []string{"a", "b", "c"}
	colIds := []string{"c1", "c2", "c3"}
	conv := buildConv(
		ddl.CreateTable{
			Name:   tableName,
			Id:     "t1",
			ColIds: colIds,
			ColDefs: map[string]ddl.ColumnDef{
				"c1": ddl.ColumnDef{Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Float64}},
				"c2": ddl.ColumnDef{Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}},
				"c3": ddl.ColumnDef{Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			}},
		schema.Table{
			Name:   tableName,
			Id:     "t1",
			ColIds: colIds,
			ColDefs: map[string]schema.Column{
				"c1": schema.Column{Name: "a", Id: "c1", Type: schema.Type{Name: "float"}},
				"c2": schema.Column{Name: "b", Id: "c2", Type: schema.Type{Name: "int"}},
				"c3": schema.Column{Name: "c", Id: "c3", Type: schema.Type{Name: "text"}},
			}})
	tableId, _ := internal.GetTableIdFromSrcName(conv.SrcSchema, tableName)
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(func(table string, cols []string, vals []interface{}) {
		rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
	})
	ProcessDataRow(conv, tableId, colIds, conv.SrcSchema[tableId], conv.SpSchema[tableId], []string{"4.2", "6", "prisoner zero"}, internal.AdditionalDataAttributes{})
	assert.Equal(t, []spannerData{spannerData{table: tableName, cols: cols, vals: []interface{}{float64(4.2), int64(6), "prisoner zero"}}}, rows)
}

func TestConvertData(t *testing.T) {
	singleColTests := []struct {
		name  string
		ty    ddl.Type
		srcTy string      // Source DB type (Used by e.g. timestamp conversions).
		in    string      // Input value for conversion.
		e     interface{} // Expected result.
	}{
		{"bool 0", ddl.Type{Name: ddl.Bool}, "", "0", false},
		{"bool 1", ddl.Type{Name: ddl.Bool}, "", "1", true},
		{"bool true", ddl.Type{Name: ddl.Bool}, "", "true", true},
		{"bool false", ddl.Type{Name: ddl.Bool}, "", "false", false},
		{"bool 5", ddl.Type{Name: ddl.Bool}, "", "5", true},
		{"bool -128", ddl.Type{Name: ddl.Bool}, "", "-128", true},
		{"bool 127", ddl.Type{Name: ddl.Bool}, "", "127", true},
		{"bytes", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, "", string([]byte{137, 80}), []byte{0x89, 0x50}}, // need some other approach to testblob type
		{"date", ddl.Type{Name: ddl.Date}, "", "2019-10-29", getDate("2019-10-29")},
		{"float64", ddl.Type{Name: ddl.Float64}, "", "42.6", float64(42.6)},
		{"int64", ddl.Type{Name: ddl.Int64}, "", "42", int64(42)},
		{"string", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, "", "eh", "eh"},
		{"datetime", ddl.Type{Name: ddl.Timestamp}, "datetime", "2019-10-29 05:30:00", getTimeWithoutTimezone(t, "2019-10-29 05:30:00")},
		{"timestamp", ddl.Type{Name: ddl.Timestamp}, "timestamp", "2019-10-29 05:30:00", getTime(t, "2019-10-29T05:30:00+05:30")},
		{"json", ddl.Type{Name: ddl.JSON}, "", "{\"key1\": \"value1\"}", "{\"key1\": \"value1\"}"},
		{"string array(set)", ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}, "", "1,Travel,3,Dance", []spanner.NullString{
			spanner.NullString{StringVal: "1", Valid: true},
			spanner.NullString{StringVal: "Travel", Valid: true},
			spanner.NullString{StringVal: "3", Valid: true},
			spanner.NullString{StringVal: "Dance", Valid: true}}},
	}
	tableName := "testtable"
	tableId := "t1"
	colId := "c1"
	for _, tc := range singleColTests {
		col := "a"
		conv := buildConv(
			ddl.CreateTable{
				Name:        tableName,
				Id:          tableId,
				ColIds:      []string{colId},
				ColDefs:     map[string]ddl.ColumnDef{colId: ddl.ColumnDef{Name: col, Id: colId, T: tc.ty, NotNull: false}},
				PrimaryKeys: []ddl.IndexKey{}},
			schema.Table{Name: tableName, Id: tableId, ColIds: []string{col}, ColDefs: map[string]schema.Column{colId: schema.Column{Name: col, Id: colId, Type: schema.Type{Name: tc.srcTy}}}})
		conv.TimezoneOffset = "+05:30"
		t.Run(tc.in, func(t *testing.T) {
			at, ac, av, err := ConvertData(conv, tableId, []string{colId}, conv.SrcSchema["t1"], conv.SpSchema["t1"], []string{tc.in}, internal.AdditionalDataAttributes{})
			checkResults(t, at, ac, av, err, tableName, []string{col}, []interface{}{tc.e}, tc.name)
		})
	}
}

func TestConvertTimestampData(t *testing.T) {
	timestampTests := []struct {
		name  string
		srcTy string
		in    string
		e     interface{}
	}{
		{"timestampt", "timestamp", "2019-10-29 05:30:00", getTime(t, "2019-10-29T05:30:00+10:00")},
		{"datetime", "datetime", "2019-10-29 05:30:00", getTimeWithoutTimezone(t, "2019-10-29 05:30:00")},
	}
	tableName := "testtable"
	tableId := "t1"
	colId := "c1"
	for _, tc := range timestampTests {
		col := "a"
		conv := buildConv(
			ddl.CreateTable{
				Name:    tableName,
				Id:      tableId,
				ColIds:  []string{colId},
				ColDefs: map[string]ddl.ColumnDef{colId: ddl.ColumnDef{Name: col, Id: colId, T: ddl.Type{Name: ddl.Timestamp}}}},
			schema.Table{
				Name:    tableName,
				Id:      tableId,
				ColIds:  []string{colId},
				ColDefs: map[string]schema.Column{colId: schema.Column{Name: col, Id: colId, Type: schema.Type{Name: tc.srcTy}}}})
		conv.TimezoneOffset = "+10:00" // Set offset so test is robust i.e. doesn't depent on local timezone.
		t.Run(tc.in, func(t *testing.T) {
			atable, ac, av, err := ConvertData(conv, tableId, []string{colId}, conv.SrcSchema[tableId], conv.SpSchema[tableId], []string{tc.in}, internal.AdditionalDataAttributes{})
			assert.Nil(t, err, tc.name)
			assert.Equal(t, atable, tableName, tc.name+": table mismatch")
			assert.Equal(t, []string{col}, ac, tc.name+": column mismatch")
			assert.Equal(t, 1, len(av))
			at, ok1 := av[0].(time.Time)
			et, ok2 := tc.e.(time.Time)
			assert.True(t, ok1 && ok2, tc.name+": cast to Time failed")
			assert.True(t, at.Equal(et), tc.name+": value mismatch")
		})
	}
}

func TestConvertMultiColData(t *testing.T) {
	multiColTests := []struct {
		name   string
		cols   []string // Input columns.
		colIds []string
		vals   []string      // Input values.
		ecols  []string      // Expected columns.
		evals  []interface{} // Expected values.
	}{
		{
			name:   "Cols in order",
			cols:   []string{"a", "b", "c"},
			colIds: []string{"c1", "c2", "c3"},
			vals:   []string{"6", "6.6", "1"},
			ecols:  []string{"a", "b", "c"},
			evals:  []interface{}{int64(6), float64(6.6), true},
		},
		{
			name:   "Cols out of order",
			cols:   []string{"b", "c", "a"},
			colIds: []string{"c2", "c3", "c1"},
			vals:   []string{"6.6", "1", "6"},
			ecols:  []string{"b", "c", "a"},
			evals:  []interface{}{float64(6.6), true, int64(6)},
		},
		{
			name:   "Null column",
			cols:   []string{"a", "b", "c"},
			colIds: []string{"c1", "c2", "c3"},
			vals:   []string{"6", "<nil>", "1"},
			ecols:  []string{"a", "c"},
			evals:  []interface{}{int64(6), true},
		},
		{
			name:   "Missing columns",
			cols:   []string{"a"},
			colIds: []string{"c1"},
			vals:   []string{"6"},
			ecols:  []string{"a"},
			evals:  []interface{}{int64(6)},
		},
	}
	tableName := "testtable"
	tableId := "t1"
	colIds := []string{"c1", "c2", "c3"}
	spTable := ddl.CreateTable{
		Name:   tableName,
		Id:     tableId,
		ColIds: colIds,
		ColDefs: map[string]ddl.ColumnDef{
			"c1": ddl.ColumnDef{Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
			"c2": ddl.ColumnDef{Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}},
			"c3": ddl.ColumnDef{Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}},
		}}
	srcTable := schema.Table{
		Name:   tableName,
		Id:     tableId,
		ColIds: colIds,
		ColDefs: map[string]schema.Column{
			"c1": schema.Column{Name: "a", Id: "c1", Type: schema.Type{Name: "int"}},
			"c2": schema.Column{Name: "b", Id: "c2", Type: schema.Type{Name: "float"}},
			"c3": schema.Column{Name: "c", Id: "c3", Type: schema.Type{Name: "bool"}},
		}}
	for _, tc := range multiColTests {
		t.Run(tc.name, func(t *testing.T) {
			conv := buildConv(spTable, srcTable)
			atable, acols, avals, err := ConvertData(conv, srcTable.Id, tc.colIds, conv.SrcSchema[tableId], conv.SpSchema[tableId], tc.vals, internal.AdditionalDataAttributes{})
			checkResults(t, atable, acols, avals, err, tableName, tc.ecols, tc.evals, tc.name)
		})
	}
}

func TestConvertError(t *testing.T) {
	errorTests := []struct {
		name string
		cols []string // Input columns.
		vals []string // Input values.
	}{
		{
			name: "Error in int64",
			cols: []string{"a", "b", "c"},
			vals: []string{" 6", "6.6", "true"},
		},
		{
			name: "Error in float64",
			cols: []string{"a", "b", "c"},
			vals: []string{"6", "6.6e", "true"},
		},
		{
			name: "Error in bool",
			cols: []string{"a", "b", "c"},
			vals: []string{"6", "6.6", "truee"},
		},
		{
			name: "Error in bool 128",
			cols: []string{"a", "b", "c"},
			vals: []string{"6", "6.6", "128"},
		},
	}
	tableName := "testtable"
	colIds := []string{"c1", "c2", "c3"}
	spTable := ddl.CreateTable{
		Name:   tableName,
		Id:     "t1",
		ColIds: colIds,
		ColDefs: map[string]ddl.ColumnDef{
			"c1": ddl.ColumnDef{Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
			"c2": ddl.ColumnDef{Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}},
			"c3": ddl.ColumnDef{Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}},
		}}
	srcTable := schema.Table{
		Name:   tableName,
		Id:     "t1",
		ColIds: colIds,
		ColDefs: map[string]schema.Column{
			"c1": schema.Column{Name: "a", Id: "c1", Type: schema.Type{Name: "int"}},
			"c2": schema.Column{Name: "b", Id: "c2", Type: schema.Type{Name: "float"}},
			"c3": schema.Column{Name: "c", Id: "c3", Type: schema.Type{Name: "bool"}},
		}}
	for _, tc := range errorTests {
		t.Run(tc.name, func(t *testing.T) {
			conv := buildConv(spTable, srcTable)
			_, _, _, err := ConvertData(conv, srcTable.Id, colIds, conv.SrcSchema[tableName], conv.SpSchema[tableName], tc.vals, internal.AdditionalDataAttributes{})
			assert.NotNil(t, err, tc.name)
		})
	}
}

func TestConvertsyntheticPKey(t *testing.T) {
	syntheticPKeyTests := []struct {
		name   string
		cols   []string // Input columns.
		colIds []string
		vals   []string      // Input values.
		ecols  []string      // Expected columns.
		evals  []interface{} // Expected values.
	}{
		{
			name:   "Sequence 0",
			cols:   []string{"a", "b", "c"},
			colIds: []string{"c1", "c2", "c3"},
			vals:   []string{"6", "6.6", "true"},
			ecols:  []string{"a", "b", "c", "synth_id"},
			evals:  []interface{}{int64(6), float64(6.6), true, "0"},
		},
		{
			name:   "Sequence 1",
			cols:   []string{"a"},
			colIds: []string{"c1"},
			vals:   []string{"7"},
			ecols:  []string{"a", "synth_id"},
			evals:  []interface{}{int64(7), fmt.Sprintf("%d", int64(bits.Reverse64(1)))},
		},
	}
	tableName := "testtable"
	tableId := "t1"

	spTable := ddl.CreateTable{
		Name:   tableName,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1": ddl.ColumnDef{Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
			"c2": ddl.ColumnDef{Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}},
			"c3": ddl.ColumnDef{Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}},
			"c4": ddl.ColumnDef{Name: "synth_id", Id: "c4", T: ddl.Type{Name: ddl.String, Len: 50}},
		}}
	srcTable := schema.Table{
		Name:   tableName,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3"},
		ColDefs: map[string]schema.Column{
			"c1": schema.Column{Name: "a", Id: "c1", Type: schema.Type{Name: "int"}},
			"c2": schema.Column{Name: "b", Id: "c2", Type: schema.Type{Name: "float"}},
			"c3": schema.Column{Name: "c", Id: "c3", Type: schema.Type{Name: "bool"}},
		}}
	conv := buildConv(spTable, srcTable)
	conv.SyntheticPKeys[spTable.Id] = internal.SyntheticPKey{ColId: "c4", Sequence: 0}
	for _, tc := range syntheticPKeyTests {
		t.Run(tc.name, func(t *testing.T) {
			atable, acols, avals, err := ConvertData(conv, srcTable.Id, tc.colIds, conv.SrcSchema[tableId], conv.SpSchema[tableId], tc.vals, internal.AdditionalDataAttributes{})
			checkResults(t, atable, acols, avals, err, tableName, tc.ecols, tc.evals, tc.name)
		})
	}
}

func buildConv(spTable ddl.CreateTable, srcTable schema.Table) *internal.Conv {
	conv := internal.MakeConv()
	conv.SpSchema[spTable.Id] = spTable
	conv.SrcSchema[srcTable.Id] = srcTable
	return conv
}

func checkResults(t *testing.T, atable string, acols []string, avals []interface{}, err error, etable string, ecols []string, evals []interface{}, name string) {
	assert.Nil(t, err, name)
	assert.Equal(t, atable, etable, name+": table mismatch")
	assert.Equal(t, ecols, acols, name+": column mismatch")
	assert.Equal(t, evals, avals, name+": value mismatch")
}

func getTime(t *testing.T, s string) time.Time {
	x, err := time.Parse(time.RFC3339, s)
	assert.Nil(t, err, fmt.Sprintf("getTime can't parse %s:", s))
	return x
}

func getTimeWithoutTimezone(t *testing.T, s string) time.Time {
	x, err := time.Parse("2006-01-02 15:04:05", s)
	assert.Nil(t, err, fmt.Sprintf("getTime can't parse %s:", s))
	return x
}

func getDate(s string) civil.Date {
	d, _ := civil.ParseDate(s)
	return d
}
