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

package common

import (
	"context"
	"fmt"
	"log"
	"sync"

	sp "cloud.google.com/go/spanner"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"

	dataproc "cloud.google.com/go/dataproc/apiv1"
	"cloud.google.com/go/dataproc/apiv1/dataprocpb"
	"google.golang.org/api/option"
)

const DefaultWorkers = 20 // Default to 20 - observed diminishing returns above this value

// InfoSchema contains database information.
type InfoSchema interface {
	GetToDdl() ToDdl
	GetTableName(schema string, tableName string) string
	GetTables() ([]SchemaAndName, error)
	GetColumns(conv *internal.Conv, table SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error)
	GetRowsFromTable(conv *internal.Conv, srcTable string) (interface{}, error)
	GetRowCount(table SchemaAndName) (int64, error)
	GetConstraints(conv *internal.Conv, table SchemaAndName) ([]string, map[string][]string, error)
	GetForeignKeys(conv *internal.Conv, table SchemaAndName) (foreignKeys []schema.ForeignKey, err error)
	GetIndexes(conv *internal.Conv, table SchemaAndName, colNameIdMp map[string]string) ([]schema.Index, error)
	ProcessData(conv *internal.Conv, tableId string, srcSchema schema.Table, spCols []string, spSchema ddl.CreateTable) error
	StartChangeDataCapture(ctx context.Context, conv *internal.Conv) (map[string]interface{}, error)
	StartStreamingMigration(ctx context.Context, client *sp.Client, conv *internal.Conv, streamInfo map[string]interface{}) error
}

// SchemaAndName contains the schema and name for a table
type SchemaAndName struct {
	Schema string
	Name   string
}

// FkConstraint contains foreign key constraints
type FkConstraint struct {
	Name    string
	Table   string
	Refcols []string
	Cols    []string
}

// ProcessSchema performs schema conversion for source database
// 'db'. Information schema tables are a broadly supported ANSI standard,
// and we use them to obtain source database's schema information.
func ProcessSchema(conv *internal.Conv, infoSchema InfoSchema, numWorkers int) error {

	GenerateSrcSchema(conv, infoSchema, numWorkers)
	initPrimaryKeyOrder(conv)
	initIndexOrder(conv)
	SchemaToSpannerDDL(conv, infoSchema.GetToDdl())
	conv.AddPrimaryKeys()
	fmt.Println("loaded schema")
	return nil
}

func GenerateSrcSchema(conv *internal.Conv, infoSchema InfoSchema, numWorkers int) error {
	tables, err := infoSchema.GetTables()
	fmt.Println("fetched tables", tables)
	if err != nil {
		return err
	}

	if numWorkers < 1 {
		numWorkers = DefaultWorkers
	}

	asyncProcessTable := func(t SchemaAndName, mutex *sync.Mutex) TaskResult[SchemaAndName] {
		table, e := processTable(conv, t, infoSchema)
		mutex.Lock()
		conv.SrcSchema[table.Id] = table
		mutex.Unlock()
		res := TaskResult[SchemaAndName]{t, e}
		return res
	}

	res, e := RunParallelTasks(tables, numWorkers, asyncProcessTable, true)
	if e != nil {
		fmt.Printf("exiting due to error: %s , while processing schema for table %s\n", e, res)
		return e
	}
	internal.ResolveForeignKeyIds(conv.SrcSchema)
	return nil
}

// ProcessData performs data conversion for source database
// 'db'. For each table, we extract and convert the data to Spanner data
// (based on the source and Spanner schemas), and write it to Spanner.
// If we can't get/process data for a table, we skip that table and process
// the remaining tables.
func ProcessData(conv *internal.Conv, infoSchema InfoSchema) {
	// Tables are ordered in alphabetical order with one exception: interleaved
	// tables appear after the population of their parent table.
	tableIds := ddl.GetSortedTableIdsBySpName(conv.SpSchema)

	for _, tableId := range tableIds {
		srcSchema := conv.SrcSchema[tableId]
		spSchema, ok := conv.SpSchema[tableId]
		if !ok {
			conv.Stats.BadRows[srcSchema.Name] += conv.Stats.Rows[srcSchema.Name]
			conv.Unexpected(fmt.Sprintf("Can't get cols and schemas for table %s:ok=%t",
				srcSchema.Name, ok))
			continue
		}
		// Extract spColds without synthetic primary key columnn id.
		colIds := RemoveSynthId(conv, tableId, spSchema.ColIds)
		err := infoSchema.ProcessData(conv, tableId, srcSchema, colIds, spSchema)
		if err != nil {
			return
		}
		if conv.DataFlush != nil {
			conv.DataFlush()
		}
	}
}

func ProcessDataWithDataproc(conv *internal.Conv, infoSchema InfoSchema, hostname string, port string, username string, pwd string) {
	// Tables are ordered in alphabetical order with one exception: interleaved
	// tables appear after the population of their parent table.
	tableIds := ddl.GetSortedTableIdsBySpName(conv.SpSchema)

	for _, tableId := range tableIds {
		srcSchema := conv.SrcSchema[tableId]
		spSchema, ok := conv.SpSchema[tableId]
		if !ok {
			conv.Stats.BadRows[srcSchema.Name] += conv.Stats.Rows[srcSchema.Name]
			conv.Unexpected(fmt.Sprintf("Can't get cols and schemas for table %s:ok=%t",
				srcSchema.Name, ok))
			continue
		}
		// Extract spColds without synthetic primary key columnn id.
		colIds := RemoveSynthId(conv, tableId, spSchema.ColIds)
		err := infoSchema.ProcessData(conv, tableId, srcSchema, colIds, spSchema)
		if err != nil {
			return
		}
		if conv.DataFlush != nil {
			conv.DataFlush()
		}
	}
}

// Function to trigger dataproc template
func TriggerDataprocTemplate(srcTable string, srcSchema string, hostname string, port string, username string, pwd string) error {
	ctx := context.Background()

	println("Triggering Dataproc template for " + srcSchema + "." + srcTable)

	// Create the batch controller cliermnt.
	batchEndpoint := fmt.Sprintf("%s-dataproc.googleapis.com:443", "us-west1")
	batchClient, err := dataproc.NewBatchControllerClient(ctx, option.WithEndpoint(batchEndpoint))

	if err != nil {
		log.Fatalf("error creating the batch client: %s\n", err)
	}

	defer batchClient.Close()

	req := &dataprocpb.CreateBatchRequest{
		Parent: "projects/yadavaja-sandbox/locations/us-west1",
		Batch: &dataprocpb.Batch{
			RuntimeConfig: &dataprocpb.RuntimeConfig{
				Version: "1.1",
			},
			EnvironmentConfig: &dataprocpb.EnvironmentConfig{
				ExecutionConfig: &dataprocpb.ExecutionConfig{
					Network: &dataprocpb.ExecutionConfig_SubnetworkUri{
						SubnetworkUri: "projects/yadavaja-sandbox/regions/us-west1/subnetworks/test-subnet1",
					},
				},
			},
			BatchConfig: &dataprocpb.Batch_SparkBatch{
				SparkBatch: &dataprocpb.SparkBatch{
					Driver: &dataprocpb.SparkBatch_MainClass{
						MainClass: "com.google.cloud.dataproc.templates.main.DataProcTemplate",
					},
					Args: []string{"--template",
						"JDBCTOSPANNER",
						"--templateProperty",
						"project.id=yadavaja-sandbox",
						"--templateProperty",
						"jdbctospanner.jdbc.url=jdbc:mysql://" + hostname + ":" + port + "/" + srcSchema + "?user=" + username + "&password=" + pwd,
						"--templateProperty",
						"jdbctospanner.jdbc.driver.class.name=com.mysql.jdbc.Driver",
						"--templateProperty",
						"jdbctospanner.sql=select * from " + srcSchema + "." + srcTable + " LIMIT 5",
						"--templateProperty",
						"jdbctospanner.output.instance=dataproc-spark-test",
						"--templateProperty",
						"jdbctospanner.output.database=eenclona-test-db",
						"--templateProperty",
						"jdbctospanner.output.table=" + srcTable,
						"--templateProperty",
						"jdbctospanner.output.primaryKey=keynumber",
						"--templateProperty",
						"jdbctospanner.output.saveMode=Append"},
					JarFileUris: []string{"file:///usr/lib/spark/external/spark-avro.jar",
						"gs://dataproc-templates-binaries/latest/java/dataproc-templates.jar",
						"gs://dataproc-templates/jars/mysql-connector-java.jar"},
				},
			},
		},
	}

	op, err := batchClient.CreateBatch(ctx, req)
	if err != nil {
		println("error creating the batch: %s\n", err)
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		println("error completing the batch: %s\n", err)
	}

	batchName := resp.GetName()

	println(batchName)
	return nil
}

// SetRowStats populates conv with the number of rows in each table.
func SetRowStats(conv *internal.Conv, infoSchema InfoSchema) {
	tables, err := infoSchema.GetTables()
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get list of table: %s", err))
		return
	}
	for _, t := range tables {
		tableName := infoSchema.GetTableName(t.Schema, t.Name)
		count, err := infoSchema.GetRowCount(t)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get number of rows for table %s", tableName))
			continue
		}
		conv.Stats.Rows[tableName] += count
	}
}

func processTable(conv *internal.Conv, table SchemaAndName, infoSchema InfoSchema) (schema.Table, error) {
	var t schema.Table
	fmt.Println("processing schema for table", table)
	tblId := internal.GenerateTableId()
	primaryKeys, constraints, err := infoSchema.GetConstraints(conv, table)
	if err != nil {
		return t, fmt.Errorf("couldn't get constraints for table %s.%s: %s", table.Schema, table.Name, err)
	}
	foreignKeys, err := infoSchema.GetForeignKeys(conv, table)
	if err != nil {
		return t, fmt.Errorf("couldn't get foreign key constraints for table %s.%s: %s", table.Schema, table.Name, err)
	}

	colDefs, colIds, err := infoSchema.GetColumns(conv, table, constraints, primaryKeys)
	if err != nil {
		return t, fmt.Errorf("couldn't get schema for table %s.%s: %s", table.Schema, table.Name, err)
	}
	colNameIdMap := make(map[string]string)
	for k, v := range colDefs {
		colNameIdMap[v.Name] = k
	}

	indexes, err := infoSchema.GetIndexes(conv, table, colNameIdMap)
	if err != nil {
		return t, fmt.Errorf("couldn't get indexes for table %s.%s: %s", table.Schema, table.Name, err)
	}

	name := infoSchema.GetTableName(table.Schema, table.Name)
	var schemaPKeys []schema.Key
	for _, k := range primaryKeys {
		schemaPKeys = append(schemaPKeys, schema.Key{ColId: colNameIdMap[k]})
	}
	t = schema.Table{
		Id:           tblId,
		Name:         name,
		Schema:       table.Schema,
		ColIds:       colIds,
		ColNameIdMap: colNameIdMap,
		ColDefs:      colDefs,
		PrimaryKeys:  schemaPKeys,
		Indexes:      indexes,
		ForeignKeys:  foreignKeys}
	return t, nil
}
