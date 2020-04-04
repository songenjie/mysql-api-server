package dal

import (
	"mysql-api-server/base"
	"log"
)

func UpdateHiveLoad(Loadlabel base.LOADLABEL) error {
	res, err := HiveLoadDb.Exec("insert into hive_load_job(Label,Cluster,Dbase,TableName,CreateTime) values ('" + Loadlabel.LabelName + "','" + Loadlabel.Cluster + "','" + Loadlabel.Database + "','" + Loadlabel.Table + "', now())")
	if err != nil {
		log.Println(err)
		return err
	}
	_, err = res.LastInsertId()
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}
func TruncateHiveLoadJob() error {
	res, err := HiveLoadDb.Exec("truncate table hive_load_job")
	if err != nil {
		log.Println(err)
		return err
	}
	_, err = res.LastInsertId()
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}
