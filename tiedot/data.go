package tiedot

import (
	"log"

	"github.com/HouzuoGuo/tiedot/db"
)

func NewDatabaseConnection(rootPath string, colName string, indexs []string) (database *db.DB) {
	database, err := db.OpenDB(rootPath)
	if err == nil {
		go initDatabase(database, colName, indexs)
	} else {
		log.Println(err)
	}

	return
}

func initDatabase(database *db.DB, colName string, indexs []string) {
	col := database.Use(colName)
	if col == nil {
		err := database.Create(colName)
		if err == nil {
			Indexs(database, colName, indexs)
		} else {
			log.Println(err)
		}
	} else {
		if len(col.AllIndexes()) != len(indexs) {
			Indexs(database, colName, indexs)
		}

	}
}

func Indexs(database *db.DB, colName string, indexs []string) {
	col := database.Use(colName)
	if col != nil {
		for _, v := range indexs {
			go col.Index([]string{v})
		}
	}
}
