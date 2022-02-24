import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.types.{DataType, StructField}
import java.text.SimpleDateFormat
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.DataFrame
import scala.util.Try
import org.apache.spark.sql.{DataFrame, Row}

import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import scala.sys.process._

object Utils {
    def printlog(msg:String)={
		System.err.println(new SimpleDateFormat("yy/MM/dd hh:mm:ss").format(System.currentTimeMillis())+msg)
    }
    val log_level : String = "DEBUG"
	def load_metadata_csv(sparkContext:SparkContext,path_maestra:String, path_estructura:String) :(Array[Array[String]], Array[Array[String]])={
		val metadata_maestra = sparkContext.textFile(path_maestra).map(x => x.split(";")).collect()
		val metadata_estructuras = sparkContext.textFile(path_estructura).map(x => x.split(";")).collect()
	 	return (metadata_maestra, metadata_estructuras)
    }

    def load_metadata_csv_hdfs(spark:SparkSession,path_maestra:String, path_estructura:String) :(Array[Array[String]], Array[Array[String]])={
        import spark.implicits._
        val metadata_maestra = spark.sql("SELECT * FROM %s".format(path_maestra)).map{row => row.toSeq.toArray.map(r=>r.toString)}.collect()
        val metadata_estructuras  = spark.sql("SELECT * FROM %s".format(path_estructura)).map{row => row.toSeq.toArray.map(r=>r.toString)}.collect()
        return (metadata_maestra, metadata_estructuras)
    }


    def load_metadata_kudu(spark:SparkSession, kudu_master:String, path_maestra:String, path_estructura:String) :(Array[Array[String]], Array[Array[String]])={
        import spark.implicits._
        val metadata_maestra = spark.read.format("org.apache.kudu.spark.kudu").options(Map("kudu.master" -> kudu_master, "kudu.table" -> path_maestra)).kudu.map { row => row.toSeq.toArray.map(r => r.toString) }.collect()
        val metadata_estructuras = spark.read.format("org.apache.kudu.spark.kudu").options(Map("kudu.master" -> kudu_master, "kudu.table" -> path_estructura)).kudu.map { row => row.toSeq.toArray.map(r => r.toString) }.collect()
        return (metadata_maestra, metadata_estructuras)
    }

    def build_query (json_paths:Array[String], column_name:Array[String]):String={
        //CAST ( expression AS data_type
        var arg_jur = "SELECT " + json_paths(0).toString + " AS " + column_name(0)
        for( x <- 1 to json_paths.length-1) {arg_jur = arg_jur + ", " + json_paths(x).toString + " AS " + column_name(x)}
        arg_jur = arg_jur + " FROM Data"
        //println("Query construida ----> %s".format(arg_jur))
        return (arg_jur)
    }
    //"SELECT * FROM DataIN WHERE %s = '%s'"
    def build_query_filters (path_identificador:String):String={
        var dato = path_identificador.split("&")
        var path_id = dato(0)
		var dato_id = dato(1)
        var arg = "SELECT * FROM DataIN WHERE " + path_id.toString + " = " + dato_id.toString
        for( x <- 1 to (dato.length-1)/2) {
            path_id = dato(x)
            dato_id = dato(x)
            //arg = arg + "," + json_paths(x).toString + " AS " + column_name(x)
        }
        arg = arg + " FROM Data"
        return (arg)
    }
    def get_maestra(identificador: String, metadata_maestra:Array[Array[String]]): (Array[String], Array[String], Array[String], Array[String]) = {
        var estructura = for (item <- metadata_maestra if item(1) == identificador) yield item(2)
        var path_identificador = for (item <- metadata_maestra if item(1) == identificador) yield item(3)
        var base_datos = for (item <- metadata_maestra if item(1) == identificador) yield item(4)
        var tabla = for (item <- metadata_maestra if item(1) == identificador) yield item(5)
        return (estructura,path_identificador,base_datos,tabla)
    }

    def get_estructuras(estructura: String, metadata_estructuras:Array[Array[String]]): (Array[String], Array[String], Array[String]) = {
        val json_path = for (item <- metadata_estructuras if item(4)== estructura) yield item(1)
        val nombre_Columna = for (item <- metadata_estructuras if item(4)== estructura) yield item(2)
        val tipo_dato = for (item <- metadata_estructuras if item(4)== estructura) yield item(3)
        return(json_path,nombre_Columna, tipo_dato)
    }

    def cast_df(df:org.apache.spark.sql.DataFrame, list_names :Array[String], list_types :Array[String], time_zone : String) : org.apache.spark.sql.DataFrame={
        if(list_names.length == list_types.length){
            var df_cast :org.apache.spark.sql.DataFrame = df
            var tipo : DataType = StringType
            for( x <- 0 to (list_names.length-1)) {
                //if (log_level=="INFO"|| log_level=="DEBUG" || log_level=="OUTPUT" ) {printlog(" Lista de tipos : " +list_types(x))}
                if(list_types(x) == "Timestamp_2"){
                    df_cast = df_cast.withColumn(list_names(x), date_format(to_timestamp(col(list_names(x)), "yyyy/MM/dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss"))
                    .withColumn(list_names(x), col(list_names(x)).cast(TimestampType))
                }
                if(list_types(x) == "Timestamp"){
                    //df_cast = df_cast.withColumn(list_names(x), col(list_names(x)).cast(StringType))
                    //df_cast = df_cast.withColumn(list_names(x), regexp_replace(col(list_names(x)), "-[0-9]+:[0-9]+", ""))
                    //df_cast = df_cast.withColumn(list_names(x), regexp_replace(col(list_names(x)), "T", " "))
                    //df_cast = df_cast.withColumn(list_names(x), from_utc_timestamp(col(list_names(x)), time_zone))
                    df_cast = df_cast.withColumn(list_names(x), col(list_names(x)).cast(TimestampType))
                    //DF_final = DF_final.withColumn("fecha_hora_kudu", from_utc_timestamp($"fecha_hora_kudu", time_zone))
                }
                if(list_types(x) == "String"){
                    df_cast = df_cast.withColumn(list_names(x), col(list_names(x)).cast(StringType))
                }
                if(list_types(x) == "Int"){
                    df_cast = df_cast.withColumn(list_names(x), col(list_names(x)).cast(IntegerType))
                }
    			if(list_types(x) == "Double"){
	                df_cast = df_cast.withColumn(list_names(x), col(list_names(x)).cast(DoubleType))
	            }
				if(list_types(x) == "Bigint"){
	                df_cast = df_cast.withColumn(list_names(x), col(list_names(x)).cast(LongType))
                }
            }
            return (df_cast)
        } else{
            //println("Error en casteo: La cantidad de columnas generadas no es igual a la cantidad de nombres parametrizados")
            return (df)
        }
    }

  	def addColumnIndex(sparkContext:org.apache.spark.sql.SQLContext, df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    	sparkContext.createDataFrame(df.rdd.zipWithIndex.map {case (row, index) => Row.fromSeq(row.toSeq :+ index)},
      	StructType(df.schema.fields :+ StructField("index", LongType, false)))
    }

    def kuduWrite(df:org.apache.spark.sql.DataFrame, kudu_master: String, kuduTable:String, log_level:String):Boolean={
        try {
            if (log_level=="DEBUG"){printlog(" INFO StreamingX: Writing data to kudu")}
			df.write.format("org.apache.kudu.spark.kudu").options(Map("kudu.master" -> kudu_master, "kudu.table" -> kuduTable)).mode("append").save()
			if (log_level=="INFO" || log_level=="DEBUG" || log_level=="OUTPUT" ){printlog(" INFO StreamingX: Successfully written: "+df.count()+" records")}
			return true
		}catch{
            case exception:Exception=>
            if (log_level=="INFO"|| log_level=="DEBUG" || log_level=="OUTPUT" ) {printlog(" ERROR StreamingX: Error writing: " +df.count()+ " records to kudu.")}
            if (log_level=="DEBUG"){System.err.println(exception)}
			return false
		}
    }

    def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
    
    def paths_revise(df: org.apache.spark.sql.DataFrame, paths:Array[String], column_name:Array[String], column_type:Array[String]):(Array[String], Array[String], Array[String])={
        var new_paths = Array[String]()
		var new_names = Array[String]()
        var new_types = Array[String]()
		for( x <-0 to paths.length-1) {
			if (hasColumn(df, paths(x).replaceAll("[0-9]]", "").replace("[","")) == true){
				new_paths = new_paths :+ paths(x)
				new_names = new_names :+ column_name(x)
				new_types = new_types :+ column_type(x)
			}
			else{
				//println ("El path no encontrado es: %s".format(paths(x)))
			}
        }
        return (new_paths, new_names, new_types)
    }

    def dividir_columna (df: org.apache.spark.sql.DataFrame, capacidad:Int, list_name_div:List[String], col_name_div:String): org.apache.spark.sql.DataFrame={
        var df_test = df.select(col("*"), substring(col(col_name_div), 1, capacidad-1).as(list_name_div(0)))
	    for( s <- 1 to (list_name_div.length-1)) {
		    df_test = df_test.select(col("*"), substring(col(col_name_div), capacidad*s, capacidad-1).as(list_name_div(s)))
	    }
        df_test = df_test.drop(col_name_div)
	    return (df_test)
    }

  def kinit(usuario_kerberos:String): Boolean ={
    val cmd = "kinit %s -kt %s.keytab".format(usuario_kerberos, usuario_kerberos)
    val output = cmd.!!
    printlog("  Kinit %s".format(usuario_kerberos))
    return true
  }
}


