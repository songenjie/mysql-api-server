package dal

import (
	"errors"
	"log"
)

func GetBackendHttpPort(Cluster string) (Host string, Port string, err error) {
	Db, err := Login(Cluster)
	if err != nil {
		return "", "", errors.New("Cannot Connect olap !" + err.Error())
	}
	defer Db.Close()
	rows, err := Db.Query("SHOW PROC '/backends';")
	if err != nil {
		return "", "", err
	}
	defer rows.Close()

	for rows.Next() {
		var Host string
		var Port string
		var i interface{}
		if err := rows.Scan(&i, &i, &Host, &i, &i,
			&i, &Port, &i, &i, &i,
			&i, &i, &i, &i, &i,
			&i, &i, &i, &i, &i); err != nil {
			return "", "", err
		}
		log.Println(Host, Port)
		if Host != "" && Port != "" {
			return Host, Port, nil
		}
	}
	return "", "", errors.New("Not Find Backend")
}

func GetFrontendHttpPort(Cluster string) (Host string, Port string, err error) {
	Db, err := Login(Cluster)
	if err != nil {
		return "", "", errors.New("Cannot Connect olap !" + err.Error())
	}
	defer Db.Close()
	rows, err := Db.Query("SHOW PROC '/frontends';")
	if err != nil {
		return "", "", err
	}
	defer rows.Close()

	for rows.Next() {
		var Host string
		var Port string
		var btrue string
		var i interface{}
		if err := rows.Scan(&i, &Host, &i, &i, &Port,
			&i, &i, &i, &btrue, &i,
			&i, &i, &i, &i, &i, &i); err != nil {
			return "", "", err
		}
		log.Println(Host, Port)
		if Host != "" && Port != "" && btrue == "true" {
			return Host, Port, nil
		}
	}
	return "", "", errors.New("Not Find Backend")
}
