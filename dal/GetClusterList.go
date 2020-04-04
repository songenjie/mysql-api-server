package dal

import (
	"mysql-api-server/base"
	"log"
	"net/http"
	"strings"
)

func GetClusterList(result *(base.GetListResult), username string) *base.ClusterListResult {
	ClusterListResult := base.ClusterListResult{}
	var bCluster = 0
	rows, err := SuperSetDb.Query("select g.name from ab_user a,ab_role b,ab_user_role c,ab_permission_view_role d,ab_permission_view e,ab_permission f,ab_view_menu g where a.id=c.user_id and " +
		"c.role_id=b.id  and b.id=d.role_id and d.permission_view_id=e.id and e.permission_id=f.id and e.view_menu_id=g.id and g.name regexp '^[^a-zA-Z0-9]'  and g.name regexp ']$'  and a.username='" + username + "';")
	if err != nil {
		result.Message = err.Error()
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var Cluster string
		if err := rows.Scan(&Cluster); err != nil {
			result.Message = err.Error()
			return nil
		}
		Cluster = strings.Split(Cluster, ".")[0]
		Cluster = Cluster[1 : len(Cluster)-1]
		log.Println(Cluster)
		bCluster = 0
		for _, cluster := range ClusterListResult.Clusters {
			if *cluster == Cluster {
				bCluster = 1
				break
			}
		}
		if bCluster == 0 {
			ClusterListResult.Clusters = append(ClusterListResult.Clusters, &Cluster)
		}
	}
	result.Code = http.StatusOK
	result.Date = ClusterListResult
	result.Message = "Success"
	return &ClusterListResult
}
