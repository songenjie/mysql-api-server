package dal

import (
	"mysql-api-server/base"
	"net/http"
)

func GetTableList(result *(base.GetListResult), Cluster string, Database string) {

	Db, err := Login(Cluster)
	if err != nil {
		result.Message = "Cannot Connect olap !" + err.Error()
	}
	defer Db.Close()

	TableListResult := base.TableListResult{
		Cluster:  Cluster,
		Database: Database,
	}
	rows, err := Db.Query("show tables from " + Database + ";")
	if err != nil {
		result.Message = err.Error()
		return
	}
	defer rows.Close()
	for rows.Next() {
		var List string
		if err := rows.Scan(&List); err != nil {
			result.Message = err.Error()
			return
		}
		TableListResult.Tables = append(TableListResult.Tables, &List)
	}
	result.Code = http.StatusOK
	result.Date = TableListResult
	result.Message = "Success"
}
