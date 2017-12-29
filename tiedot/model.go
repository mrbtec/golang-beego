package tiedot

import (
	"encoding/json"
	"log"

	"github.com/HouzuoGuo/tiedot/db"
)

type GenericRepository struct {
	database *db.DB
	coll     string
}

type GenericModel map[string]interface{}

func NewGenericRepository(database *db.DB, coll string) *GenericRepository {
	col := database.Use(coll)
	if col == nil {
		err := database.Create(coll)
		if err != nil {
			log.Println(err)
		}

		col = database.Use(coll)
	}

	return &GenericRepository{database, coll}
}

func hydrateGeneric(rawGeneric map[string]interface{}) GenericModel {
	return GenericModel(rawGeneric)
}

func flattenGeneric(generic GenericModel) map[string]interface{} {
	return map[string]interface{}(generic)
}

func (repo *GenericRepository) FetchAll() []GenericModel {
	col := repo.database.Use(repo.coll)
	generics := make([]GenericModel, 0)
	col.ForEachDoc(func(id int, doc []byte) (moveOn bool) {
		moveOn = true

		val := make(map[string]interface{}, 0)

		json.Unmarshal(doc, &val)
		generic := hydrateGeneric(val)
		generic["ID"] = id

		generics = append(generics, generic)

		return
	})

	return generics
}

func (repo *GenericRepository) Fetch(id int) GenericModel {
	col := repo.database.Use(repo.coll)

	rawGeneric, err := col.Read(id)
	if err != nil {
		log.Println(err)
	}
	generic := hydrateGeneric(rawGeneric)
	generic["ID"] = id

	return generic
}

func (repo *GenericRepository) Save(generic GenericModel) {
	col := repo.database.Use(repo.coll)
	data := flattenGeneric(generic)

	if _, ok := generic["ID"]; !ok {
		id, err := col.Insert(data)
		generic["ID"] = id
		if err != nil {
			log.Println("An error occurred while inserting the model: ", err)
		}

	} else {
		err := col.Update(generic["ID"].(int), data)
		if err != nil {
			log.Println("An error occurred while updating the model: ", err)
		}
	}
}

func (repo *GenericRepository) Query(query interface{}) []GenericModel {
	col := repo.database.Use(repo.coll)
	result := make(map[int]struct{})
	if err := db.EvalQuery(query, col, &result); err != nil {
		log.Println(err)
	}

	generics := make([]GenericModel, 0)
	for id := range result {
		generics = append(generics, repo.Fetch(id))
	}

	return generics
}

func (repo *GenericRepository) Index(cols []string) {
	col := repo.database.Use(repo.coll)
	for _, v := range cols {

		if err := col.Index([]string{v}); err != nil {
			log.Println(err)
		}
	}
}

func (repo *GenericRepository) Delete(id int) {
	col := repo.database.Use(repo.coll)
	col.Delete(id)
}
