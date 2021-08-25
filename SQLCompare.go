package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"text/tabwriter"
)

type Column struct {
	Name  string
	Type  string
	Other string
}

type Index struct {
	Name       string
	ColumnName string
}

type Constraint struct {
	Name  string
	Type  string
	Other string
}

type Table struct {
	Name        string
	Columns     map[string]Column
	Indexes     map[string]Index
	Constraints map[string][]Constraint
}

const (
	MissingTable         = "MISSING_TABLE"
	MissingColumn        = "MISSING_COLUMN"
	WrongColumnType      = "WRONG_COLUMN_TYPE"
	WrongColumnOther     = "WRONG_COLUMN_OTHER"
	MissingIndex         = "MISSING_INDEX"
	MissingConstraint    = "MISSING_CONSTRAINT"
	WrongConstraintOther = "WRONG_CONSTRAINT_OTHER"
)

type Diff struct {
	Type   string
	Target string
	A      string
	B      string
}

func main() {

	args := os.Args
	if len(args) < 2 {
		log.Fatal("missing first file path")
	}

	if len(args) < 3 {
		log.Fatal("missing second file path")
	}

	d1, err := ioutil.ReadFile(args[1])
	if err != nil {
		log.Fatal(fmt.Sprintf("error reading file 1: %s, %v", args[1], err))
	}

	d2, err := ioutil.ReadFile(args[2])
	if err != nil {
		log.Fatal(fmt.Sprintf("error reading file 2: %s, %v", args[2], err))
	}

	dataA := string(d1)
	dataB := string(d2)

	tablesA := parseTables(dataA)
	tablesB := parseTables(dataB)

	//	printTables(tablesA)
	//printTables(tablesB)

	diffs := compareTables(tablesA, tablesB)
	printDiffs(diffs, args[1], args[2])
}

func compareTables(tableMapA map[string]Table, tableMapB map[string]Table) []Diff {

	diffs := make([]Diff, 0)

	for _, tableA := range tableMapA {

		tableB, tableExists := tableMapB[tableA.Name]
		if !tableExists {
			diffs = append(diffs, Diff{
				Type:   MissingTable,
				Target: tableA.Name,
				A:      tableA.Name,
				B:      "",
			})
			continue
		}

		for _, columnA := range tableA.Columns {

			columnB, columnExists := tableB.Columns[columnA.Name]
			if !columnExists {
				diffs = append(diffs, Diff{
					Type:   MissingColumn,
					Target: tableA.Name,
					A:      columnA.Name,
					B:      "",
				})
				continue
			}

			if columnA.Type != columnB.Type {
				diffs = append(diffs, Diff{
					Type:   WrongColumnType,
					Target: fmt.Sprintf("%s.%s", tableA.Name, columnA.Name),
					A:      columnA.Type,
					B:      columnB.Type,
				})
			}

			if columnA.Other != columnB.Other {
				diffs = append(diffs, Diff{
					Type:   WrongColumnOther,
					Target: fmt.Sprintf("%s.%s", tableA.Name, columnA.Name),
					A:      columnA.Other,
					B:      columnB.Other,
				})
			}
		}
	}

	return diffs
}

func parseTables(data string) map[string]Table {
	var table Table
	tables := make(map[string]Table)
	var analyzingTable bool

	keywords := []string{"PRIMARY", "KEY", "CONSTRAINT"}
	isKeyword := func(str string) bool {
		for _, v := range keywords {
			if str == v || str == "" || str == "--" {
				return true
			}
		}

		return false
	}

	res := strings.Split(data, "\n")
	for _, value := range res {

		value = strings.Trim(value, " ")
		infos := strings.Split(value, " ")

		if len(infos) > 1 && infos[1] == "ENGINE=InnoDB" {
			continue
		}

		if infos[0] == "CREATE" && infos[1] == "TABLE" {
			if analyzingTable {
				tables[table.Name] = table
			}
			analyzingTable = true
			tableName := strings.Trim(infos[2], "`")
			cols := make(map[string]Column)
			indexes := make(map[string]Index)
			constraints := make(map[string][]Constraint)
			table = Table{Name: tableName, Columns: cols, Indexes: indexes, Constraints: constraints}
			continue
		}

		//column definition
		if analyzingTable && !isKeyword(infos[0]) {

			other := strings.Trim(strings.Join(infos[2:], " "), ",")
			name := strings.Trim(infos[0], "`")

			column := Column{
				Name:  name,
				Type:  infos[1],
				Other: other,
			}

			table.Columns[name] = column

			continue
		}
	}
	tables[table.Name] = table

	return tables
}

func printTables(tables map[string]Table) {
	for _, table := range tables {
		fmt.Print("\n\n")
		printTable(table)
	}
}

func printTable(table Table) {
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
	fmt.Println("Table: ", table.Name)
	for _, col := range table.Columns {

		fmt.Fprintf(w, "%v\t|\t%v\t|\t%v\n", col.Name, col.Type, col.Other)
	}
	w.Flush()
}

func printDiffs(diffs []Diff, aFileName string, bFileName string) {
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
	fmt.Printf("\n\nDiffs\n\n")
	fmt.Fprintf(w, "Type\t|\tTarget\t|\t%s\t|\t%s\n", aFileName, bFileName)
	for _, diff := range diffs {

		fmt.Fprintf(w, "%v\t|\t%v\t|\t%v\t|\t%v\n", diff.Type, diff.Target, diff.A, diff.B)
	}

	w.Flush()
	fmt.Println()
}
