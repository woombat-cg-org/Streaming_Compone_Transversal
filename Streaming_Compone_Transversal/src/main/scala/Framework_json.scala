import org.apache.spark.sql.Row
import java.io._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructField}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.rdd.RDD
import java.sql.Timestamp

import java.sql.Timestamp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.orc._

//import spark.implicits._
//val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//val Metadata = new Read_Metadata

object Framework_json{
	def main(args: Array[String]) {
		if (args.length < 1) {
			System.err.println(
				"""
				|Uso: streamingX <brokers> <topico>
				|  <topico> Topico Kafka a consumir
				|
			""".stripMargin)
			System.exit(1)
		}
		val topicSet : Set[String] = Set(args(0))

		//iniciar variables
		val (log_level, spark_master, path_maestra, path_estructura, path_batch, kudu_master, ss_batch_secs, spark_log_level, brokers, group_id, security, sasl_mechanism, identificador, capacidad, list_name_div, col_name_div, time_zone, usuario_kerberos) = StreamingInit.configInit()
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: Variables inicializadas")}
		if (log_level=="INFO" || log_level=="DEBUG") {Utils.printlog(" INFO StreamingX: Inicio del proceso, topico(s) a consumir: "+ args(0))}

		Utils.kinit(usuario_kerberos) // kinit

		//iniciar sesion y contexto de spark
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Iniciando session spark")}   
		val spark = SparkSession.builder.master(spark_master).getOrCreate()
		spark.sparkContext.setLogLevel(spark_log_level)
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Spark iniciado correctamente")} 
		  
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Iniciando contexto de spark")}   
		import spark.implicits._
		val sc = spark.sparkContext
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Session y contexto de spark creado")}

		// Inicializando SparkStreaming 
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: Inicializando SparkStreaming")}
		val streamingContext = new StreamingContext(spark.sparkContext, Milliseconds(ss_batch_secs.toLong))
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: SparkStreaming iniciado correctamente")}
		val kafkaParams = Map[String, Object](
		"bootstrap.servers" -> brokers,
		"security.protocol" -> security,
		"sasl.mechanism" -> sasl_mechanism,
		"sasl.kerberos.service.name" -> "kafka",
		"auto.offset.reset" -> "latest",
		"key.deserializer" -> classOf[StringDeserializer],
		"value.deserializer" -> classOf[StringDeserializer],
		"group.id" -> group_id,
		"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		// ======================= Inicializando Kafka Streaming ==================================
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: Inicializando Kafka Streaming")}
		val messages = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Subscribe[String, String](topicSet, kafkaParams))
		if (log_level=="INFO"|| log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: Kafka Streaming iniciado correctamente")}

		// iniciar metadata 
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Empezando a leer metadata")} 
		val (metadata_maestra, metadata_estructuras) = Utils.load_metadata_kudu(spark, kudu_master, path_maestra, path_estructura)
		//val (metadata_maestra, metadata_estructuras) = Utils.load_metadata_csv(sc, path_maestra, path_estructura)
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Get_maestra IDENTIFICADOR: %s".format(identificador))} 

		val(estructura,path_identificador,base_datos,tabla) = Utils.get_maestra(identificador, metadata_maestra)
		if (log_level=="INFO"|| log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: Metadata iniciada correctamente")}

		
		// ======================= Inicializando el streaming de Jsons ==================================   
		//if (log_level=="DEBUG") {messages.map(record => record.value().toString).print()} 
		if (log_level=="INFO"|| log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: Listo para recibir")}
		messages.foreachRDD{(rdd: RDD[ConsumerRecord[String, String]], t: Time) =>
			if (!rdd.isEmpty()) {
				//if (log_level=="DEBUG") {Utils.printlog("DEBUG: Mensaje Nuevo")}
				var saved:Boolean = true
				val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
				try {
					// ======================= Inicializando la creacion del batch ================================== 
					var json_record = rdd.map(record => (record.value()).toString.replaceAll("^[a-zA-Z0-9:	]+\\{", "\\{"))
					var df_json = spark.read.json(json_record)

					//df_json = Utils.addColumnIndex(spark.sqlContext, df_json)
					//df_json = df_json.withColumn("fecha_hora_kafka", lit (new Timestamp(t.milliseconds)))
					//df_json = df_json.withColumn("fecha_hora_kafka", from_utc_timestamp($"fecha_hora_kafka", time_zone))
					if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Mensaje nuevo! Cantidad de tramas en StreamingBatch: %s".format(df_json.count()))}

					if (df_json.columns.contains("jsonPayload")){
						df_json = df_json.select("jsonPayload")
						df_json.createOrReplaceTempView("DataIN")
						if (df_json.count()>0){
							if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Cantidad de tramas en Componente: %s".format(df_json.count()))}
							for( x <- 0 to estructura.length-1) {
								val(json_paths, column_name, column_type) = Utils.get_estructuras(estructura(x), metadata_estructuras)
								var kuduTable: String = "impala::%s.%s".format(base_datos(x) , tabla(x))
								var query_prefiltro = path_identificador(x)
								var DF_Temporal = spark.sql(query_prefiltro)
								if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Filtro para tabla en: %s, Cantidad de tramas: %s".format(tabla(x), DF_Temporal.count()))}
								if(DF_Temporal.count()>0){
									var (new_paths, new_names, new_types) = Utils.paths_revise(DF_Temporal, json_paths, column_name, column_type)
									var query = Utils.build_query(new_paths, new_names)
									DF_Temporal.createOrReplaceTempView("Data")
									var DF_query = spark.sql(query)
									val DF_casteado= Utils.cast_df(DF_query, new_names, new_types, time_zone)
									var DF_final = DF_casteado.withColumn("fecha_hora_kudu", current_timestamp())
									println("------- DF FINAL primera parte--------")
									DF_final.show()
									//DF_final =  DF_final.withColumn("periodo", lit(new SimpleDateFormat("yyyyMMdd").format(System.currentTimeMillis()).toInt).cast("int"))
									//DF_final = DF_final.withColumn("periodo", from_utc_timestamp($"fecha_hora", time_zone))
									//DF_final = DF_final.withColumn("periodo", $"fecha_hora", time_zone)
									//
									//DF_final = DF_final.withColumn("fecha_hora_operacion", from_utc_timestamp($"fecha_hora_operacion", time_zone))
									DF_final = DF_final.withColumn("fecha_hora_kudu", col("fecha_hora_kudu").cast(StringType))
									DF_final.select("fecha_hora_kudu").show(false)
									//DF_final = DF_final.withColumn("fecha_hora_operacion", regexp_replace(col("fecha_hora_operacion"), "-[0-9]+:[0-9]+", ""))
									//DF_final = DF_final.withColumn("fecha_hora_operacion", regexp_replace(col("fecha_hora_operacion"), "T", " "))
									DF_final = DF_final.withColumn("periodo", regexp_replace(col("fecha_hora_kudu"), "\\.[0-9]+", ""))
									DF_final.select("fecha_hora_kudu","periodo").show(false)
									DF_final = DF_final.withColumn("periodo", regexp_replace(col("fecha_hora_kudu"), " [0-9]+:[0-9]+:[0-9]+", ""))
									DF_final.select("fecha_hora_kudu","periodo").show(false)
									//DF_final = DF_final.withColumn("periodo", regexp_replace(col("fecha_hora_operacion"), " [0-9]+:[0-9]+:[0-9]+.[0-9]+", ""))
									DF_final = DF_final.withColumn("periodo", regexp_replace(col("periodo"), "-", ""))
									DF_final.select("fecha_hora_kudu","periodo").show(false)
									DF_final = DF_final.withColumn("periodo",  col("periodo").cast("int"))
									DF_final.select("fecha_hora_kudu","periodo").show(false)
									DF_final = DF_final.withColumn("fecha_hora_kudu",col("fecha_hora_kudu").cast(TimestampType))
									DF_final = DF_final.withColumn("fecha_hora_operacion", from_utc_timestamp($"fecha_hora_operacion", time_zone))
									DF_final = DF_final.withColumn("fecha_hora_kudu", from_utc_timestamp($"fecha_hora_kudu", time_zone))
									//DF_final = DF_final.drop("fecha_hora_operacion_cast")//
									println("------- DF FINAL --------")
									DF_final.show()
									if(Utils.hasColumn(DF_final, col_name_div) == true){ DF_final = Utils.dividir_columna(DF_final, capacidad.toInt, list_name_div, col_name_div)}
									var estado = Utils.kuduWrite(DF_final, kudu_master, kuduTable, log_level)
									if (estado){messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)}
								}
							}
						}
					}
					else{
						if (log_level=="DEBUG") {Utils.printlog(" DEBUG: No hay tramas para componente: %s".format(df_json.count()))}
					}
				}
				catch{
					case exception:Exception=>
					if (log_level=="INFO"|| log_level=="DEBUG") {Utils.printlog(" ERROR StreamingX: Error processing stream.")}
					if (log_level=="DEBUG"){System.err.println(exception)}
					//messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
				}
				//finally{
				
				//}
			}
		}
		streamingContext.start()
		streamingContext.awaitTermination()
	}
}