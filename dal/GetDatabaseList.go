package dal

import (
	"mysql-api-server/base"
	"log"
	"net/http"
	"strings"
)

func GetDatabaseList(result *(base.GetListResult), username string, Cluster string) *base.DatabaseListResult {
	DatabaseListResult := base.DatabaseListResult{
		Cluster: Cluster,
	}
	rows, err := SuperSetDb.Query("select g.name from ab_user a,ab_role b,ab_user_role c,ab_permission_view_role d,ab_permission_view e,ab_permission f," +
		"ab_view_menu g where a.id=c.user_id and c.role_id=b.id  and b.id=d.role_id and d.permission_view_id=e.id and e.permission_id=f.id and e.view_menu_id=g.id " +
		"and g.name regexp ']$' and a.username='" + username + "'  and g.name regexp '^\\\\[" + Cluster + "\\\\]..*';")
	if err != nil {
		result.Message = err.Error()
		return &DatabaseListResult
	}
	defer rows.Close()
	for rows.Next() {
		var Database string
		if err := rows.Scan(&Database); err != nil {
			result.Message = err.Error()
			return &DatabaseListResult
		}
		Database = strings.Split(Database, ".")[len(strings.Split(Database, "."))-1]
		Database = Database[1 : len(Database)-1]
		log.Println(Database)
		if Database != "information_schema" {
			DatabaseListResult.Databases = append(DatabaseListResult.Databases, &Database)
		}
	}
	result.Code = http.StatusOK
	result.Date = DatabaseListResult
	result.Message = "Success"
	return &DatabaseListResult
}
