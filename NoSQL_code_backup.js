//Importar movies
const contents = [
    {
        content: "C:\\Users\\Constanza\\Downloads\\movies.json",
        collection: "movies",
        idPolicy: "overwrite_with_same_id", //overwrite_with_same_id|always_insert_with_new_id|insert_with_new_id_if_id_exists|skip_documents_with_existing_id|abort_if_id_already_exists|drop_collection_first|log_errors
        //Use the transformer to customize the import result
        //transformer: (doc)=>{ //async (doc)=>{
        //   doc["importDate"]= new Date();
        //   doc["oid"] = mb.convert({input:doc["oid"], to:"objectId", onError:"remain_unchanged", onNull:null});  //to: double|string|objectId|bool|date|int|long|decimal
        //   return doc; //return null skips this doc
        //}
    }
];

mb.importContent({
    connection: "localhost",
    database: "Tarea",
    fromType: "file",
    batchSize: 2000,
    contents
})

//Analizar con find la colección.
db.movies.find({})

//Contar cuántos documentos (películas) tiene cargado
db.movies.countDocuments({})

//Insertar una película
db.movies.insertOne({"title" : "Test", "year" : 2023, "cast" : [  ], "genres" : [  ]})

//Borrar la película insertada en el punto anterior (en el 3)
db.movies.deleteOne({"title" : "Test", "year" : 2023, "cast" : [  ], "genres" : [  ]})

//Contar cuantas películas tienen actores (cast) que se llaman “and”
var fil = db.movies.find({"cast": { $in: ["and"]}})
fil.count()

//Actualizar los documentos cuyo actor (cast) tenga por error el valor “and” como si realmente fuera un actor
db.movies.updateMany({},{$pull: {"cast": { $in: ["and"]}}})

//Contar cuantos documentos (películas) tienen el array ‘cast’ vacío
var cast_null = db.movies.find({"cast": {$in: [null, []]}})
cast_null.count()

//Actualizar TODOS los documentos (películas) que tengan el array cast vacío, añadiendo un nuevo elemento dentro
//del array con valor Undefined.
db.movies.updateMany({"cast": {$in: [null, []]}}, {$push: { cast: "Undefined"}})

//Contar cuantos documentos (películas) tienen el array genres vacío
var genres_null = db.movies.find({"genres": {$in: [null, []]}})
genres_null.count()

//Actualizar TODOS los documentos (películas) que tengan el array genres vacío, añadiendo un nuevo elemento dentro
//del array con valor Undefined.
db.movies.updateMany({"genres": {$in: [null, []]}}, {$push: { genres: "Undefined"}})

//Mostrar el año más reciente / actual que tenemos sobre todas las películas
db.movies.aggregate([{ $group : { _id: null, year: { $max : "$year"}}}])

//Contar cuántas películas han salido en los últimos 20 años
db.movies.aggregate([{$match: {year: {$gte: 1998}}},{$count: "movies_last20years"}])

// Contar cuántas películas han salido en la década de los 60 (del 60 al 69 incluidos).
db.movies.aggregate([{$match: {year: {$gte: 1960, $lte: 1969}}},{$count: "movies_60s"}])

//Mostrar el año u años con más películas mostrando el número de películas de ese año.
db.movies.aggregate([{$group: {"_id": "$year", "n_peliculas": { $sum: 1 }}},{ $sort: {"n_peliculas": -1}}]).limit(1)

//Mostrar el año u años con menos películas mostrando el número de películas de ese año.
db.movies.aggregate([{$group: {"_id": "$year", "n_peliculas": { $sum: 1 }}},{ $sort: {"n_peliculas": +1}}]).limit(3)

//Guardar en nueva colección llamada “actors” realizando la fase $unwind por actor.
db.movies.aggregate([{$project: {"_id": false }}, {$unwind: "$cast"}, { $out: "actors" }])
db.actors.countDocuments()

//Sobre actors (nueva colección), mostrar la lista con los 5 actores que han participado en más películas mostrando el
//número de películas en las que ha participado.
db.actors.aggregate([
    {$group: {"_id": "$cast", "n_peliculas": { $sum: 1 } }}])
    .match({"_id": {$ne: "Undefined" }})
    .sort({"n_peliculas":-1}).limit(5);

//Sobre actors (nueva colección), agrupar por película y año mostrando las 5 en las que más actores hayan
//participado, mostrando el número total de actores.
db.actors.aggregate([{$match: {"cast":{ $ne: "Undefined" }}}, 
{$group: {"_id": {"title": "$title", "year": "$year"}, "n_actores": {$sum: 1}}}])
.sort({"n_actores":-1}).limit(5);

//Sobre actors (nueva colección), mostrar los 5 actores cuya carrera haya sido la más larga.
db.actors.aggregate([{$match: {"cast":{ $ne: "Undefined" }}},
{$group: { "_id": "$cast", "start": { $min: "$year" }, "finish": { $max: "$year" }}}, 
{$addFields:{"trayectoria": { $subtract: ["$finish", "$start"]}}}]).sort({"trayectoria":-1}).limit(5);

//Sobre actors (nueva colección), Guardar en nueva colección llamada “genres” realizando la fase $unwind por
//genres. Después, contar cuantos documentos existen en la nueva colección
db.actors.aggregate([{$project: {"_id": false }}, {$unwind: "$genres"}, { $out: "genres" }])
db.genres.countDocuments()

//Sobre genres (nueva colección), mostrar los 5 documentos agrupados por “Año y Género” que más número de
//películas diferentes tienen mostrando el número total de películas.
db.genres.aggregate([{"$group": {"_id": {"anio": "$year","genre": "$genres"},
            "totalmovie": { "$addToSet": "$title" }}},
    {"$project": {"totalmovie": { "$size": "$totalmovie" }}}]).sort({"totalmovie":-1}).limit(5);

// Sobre genres (nueva colección), mostrar los 5 actores y los géneros en los que han participado con más número de
//géneros diferentes, se debe mostrar el número de géneros diferentes que ha interpretado.
db.genres.aggregate([{$match: {"cast":{ $ne: "Undefined" }}},
{"$group":  {"_id":"$cast", generos: {$addToSet: "$genres"}}},
 {$project: {_id: 1, n_generos: {$size:"$generos"}, "generos":1}}])
 .sort({"n_generos":-1}).limit(5);
 
 //Sobre genres (nueva colección), mostrar las 5 películas y su año correspondiente en los que más géneros diferentes
//han sido catalogados, mostrando esos géneros y el número de géneros que contiene.
db.genres.aggregate([{"$group": {"_id": {"titulo": "$title","anio": "$year"},
            "generos": { "$addToSet": "$genres" }}},
    {$project: {_id: 1, n_generos: {$size:"$generos"}, "generos":1}}]).sort({"n_generos":-1}).limit(5);
    
//Revisar los generos unicos
db.genres.distinct("genres")

//Ver cuantos actores se llaman Henry
db.actors.distinct("cast", {"cast": /^Henry/}).length

//Cual(es) es(son) la(s) pelicula(s) de crimen mas antigua, considerando que mas de una pelicula puede haber salido en el mismo anio
db.movies.aggregate([
  {$match: { genres: {$in:["Crime"]}}},
  {$sort: { year: 1 } },
  {$group: { _id: "$title", year: { $first: "$year" } } },
  {$group: { _id: "$year", movies: { $push: { title: "$_id", year: "$year" } } } },
  {$sort: { _id: 1 } },
  {$limit: 1 },
  {$unwind: "$movies" },
  {$replaceRoot: { newRoot: "$movies" } }
])

