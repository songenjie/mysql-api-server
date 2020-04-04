package dal

import (
	"mysql-api-server/base"
	"net/http"
)

func GetPartitionList(result *(base.GetListResult), Cluster string, Database string, Table string) {
	Db, err := Login(Cluster)
	if err != nil {
		result.Message = "Cannot Connect olap !" + err.Error()
	}
	defer Db.Close()

	PartionListResulst := base.PartiotionsListResult{
		Cluster:  Cluster,
		Database: Database,
		Table:    Table,
	}
	rows, err := Db.Query("show partitions from " + Database + "." + Table + ";")
	if err != nil {
		result.Message = err.Error()
		return
	}
	defer rows.Close()
	for rows.Next() {
		var List string
		var i string
		if err := rows.Scan(&i, &List, &i, &i, &i, &i, &i, &i, &i, &i, &i, &i, &i, &i); err != nil {
			result.Message = err.Error()
			return
		}
		if List != Table {
			PartionListResulst.Partitions = append(PartionListResulst.Partitions, &List)
		}
	}
	result.Code = http.StatusOK
	result.Date = PartionListResulst
	result.Message = "Success"
}
