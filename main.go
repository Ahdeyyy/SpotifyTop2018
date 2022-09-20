package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// song struct
type Song struct {
	id               string
	name             string
	artists          string
	danceability     float64
	energy           float64
	key              int
	loudness         float64
	mode             int
	speechiness      float64
	acousticness     float64
	instrumentalness float64
	liveness         float64
	valence          float64
	tempo            float64
	duration_ms      int
	time_signature   int
}

func stringToFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(err)
	}
	return f
}

func stringToInt(s string) int {
	s = strings.TrimSuffix(s, ".0")
	i, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return i
}

// parseCsv parses a csv file and returns a 2D slice of strings for the data and a slice of strings for the headers
func parseCsv(filepath string) ([][]string, []string) {

	file, err := os.Open(filepath)

	if err != nil {
		fmt.Println(err)
	}

	defer file.Close()

	reader := csv.NewReader(file)

	var records [][]string
	for {

		record, err := reader.Read()
		// if we've reached the end of the file, break
		if err == io.EOF {
			break
		}

		if err != nil {
			fmt.Println(err)
		}

		records = append(records, record)
	}
	// remove the first line
	return records[1:], records[0]

}

// parseSong takes a slice of strings and returns a slice of song struct
func parseSong(records [][]string) []Song {
	var songs []Song
	for _, record := range records {
		song := Song{
			id:               record[0],
			name:             record[1],
			artists:          record[2],
			danceability:     stringToFloat(record[3]),
			energy:           stringToFloat(record[4]),
			key:              stringToInt(record[5]),
			loudness:         stringToFloat(record[6]),
			mode:             stringToInt(record[7]),
			speechiness:      stringToFloat(record[8]),
			acousticness:     stringToFloat(record[9]),
			instrumentalness: stringToFloat(record[10]),
			liveness:         stringToFloat(record[11]),
			valence:          stringToFloat(record[12]),
			tempo:            stringToFloat(record[13]),
			duration_ms:      stringToInt(record[14]),
			time_signature:   stringToInt(record[15]),
		}
		songs = append(songs, song)
	}
	return songs
}

// open the database
func openDatabase() *sql.DB {
	if _, err := os.Stat("songs.db"); err == nil {
		// database exists
		db, err := sql.Open("sqlite3", "./songs.db")
		if err != nil {
			fmt.Println(err)
		}
		return db

	 } else {
		// database does not exist so create a table and return the database
		db, err := sql.Open("sqlite3", "./songs.db")
		if err != nil {
			fmt.Println(err)
		}
		createTable(db)
		return db
	 }
}

// create table in database
func createTable(db *sql.DB) {

	sqlStmt := `
  CREATE TABLE IF NOT EXISTS songs (
    id TEXT NOT NULL PRIMARY KEY,
    name TEXT,
    artists TEXT,
    danceability FLOAT,
    energy FLOAT,
    key INT,
    loudness FLOAT,
    mode INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    duration_ms INT,
    time_signature INT
  );
  `
	_, err := db.Exec(sqlStmt)
	if err != nil {
		fmt.Println(err)
	}
}

// insert data into database
func insertData(db *sql.DB, songs []Song) {
  	for _, song := range songs {

		sqlStmt := `
    INSERT INTO songs(id, name, artists, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, duration_ms, time_signature) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
		_, err := db.Exec(sqlStmt, song.id, song.name, song.artists, song.danceability, song.energy, song.key, song.loudness, song.mode, song.speechiness, song.acousticness, song.instrumentalness, song.liveness, song.valence, song.tempo, song.duration_ms, song.time_signature)
		if err != nil {
			fmt.Println(err)
		}
	}
}

// get all data in database
func getData(db *sql.DB) []Song {
	rows, err := db.Query("SELECT * FROM songs")
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	var songs []Song
	for rows.Next() {
		var song Song
		err := rows.Scan(&song.id, &song.name, &song.artists, &song.danceability, &song.energy, &song.key, &song.loudness, &song.mode, &song.speechiness, &song.acousticness, &song.instrumentalness, &song.liveness, &song.valence, &song.tempo, &song.duration_ms, &song.time_signature)
		if err != nil {
			fmt.Println(err)
		}
		songs = append(songs, song)
	}
	err = rows.Err()
	if err != nil {
		fmt.Println(err)
	}
	return songs
}

// search database for artist
func findArtist(db *sql.DB, artist string) []Song {
	
	sqlStmt := `
	SELECT * FROM songs WHERE artists LIKE ?
	`
	rows, err := db.Query(sqlStmt, "%"+artist+"%")
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	var songs []Song
	for rows.Next() {
		var song Song
		err := rows.Scan(&song.id, &song.name, &song.artists, &song.danceability, &song.energy, &song.key, &song.loudness, &song.mode, &song.speechiness, &song.acousticness, &song.instrumentalness, &song.liveness, &song.valence, &song.tempo, &song.duration_ms, &song.time_signature)
		if err != nil {
			fmt.Println(err)
		}
		songs = append(songs, song)
	}
	err = rows.Err()
	if err != nil {
		fmt.Println(err)
	}
	return songs
}

func main() {

	db := openDatabase()
	defer db.Close()
	// query data
	songs := findArtist(db, "Drake")

	for _, song := range songs {
		fmt.Println(song)
	}
	
}
