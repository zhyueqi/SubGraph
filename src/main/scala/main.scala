import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.graphx.impl.{ EdgePartitionBuilder, GraphImpl }
import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.graphx._
import com.typesafe.config._
import simplelib._

object SubGraph {

    // Load our own config values from the default location, application.conf
    val conf = ConfigFactory.load()

    val context = new SimpleLibContext()

    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Simple Application")
        val sc = new SparkContext(conf)

        if (args.length < 1) {
            println("【参数一】运行程序编号，【参数...】");
            return
        }

        def task(x: String) = x match {
            case "1" => {
                if (args.length >= 3) {
                    remove_repeating_vertice(sc, args(1), args(2))
                }
            }
            case "2" => {
                if (args.length >= 3) {
                    remove_repeating_edges(sc, args(1), args(2))
                }
            }
            case "3" => {
                if (args.length >= 4) {
                    joinTable(sc, args(1), args(2), args(3))
                }
            }
            case "4" => {
                if (args.length >= 3) {
                    connectedComponents(sc, args(1), args(2))
                }
            }
            case _ => {
                println("【1】：清理节点；【2】：清理边；【3】：补充Id；【4】：求子图 （填入【】中数字）")
            }
        }

        task(args(0));

        //去除重复项数据
        //remove_repeating_vertice(sc,pathOfSrc,pathOfDst);
        // remove_repeating_edges(sc,pathOfSrc,pathOfDst);

        //补充 id
        //joinTable(sc,pathOfSrc,pathOfDst);
        //无向图子图
        //connectedComponents(sc,pathOfSrc,pathOfDst);
    }

    def remove_repeating_vertice(sc: SparkContext, src: String, dst: String) {

        val lines = sc.textFile(src)
        val lineTrips = lines.map(line => line.split(",").map(elem => elem.trim))
        // lineTrips.persist()
        val combined = combine_a(lineTrips);
        println(combined.count());
        writeToFile(combined, dst);
        merge(dst, dst + ".csv")
    }

    def remove_repeating_edges(sc: SparkContext, src: String, dst: String) {
        val lines = sc.textFile(src)
        val lineTrips = lines.map(line => line.split(",").map(elem => elem.trim))
        // lineTrips.persist()
        val combined = combine_b(lineTrips);
        println(combined.count());
        writeToFile(combined, dst);
        merge(dst, dst + ".csv")
    }

    def joinTable(sc: SparkContext, src_vertice: String, src_relation: String, dst: String) {
        val lines_post = sc.textFile(src_vertice)
        val lines_relation = sc.textFile(src_relation)

        val rows_post = lines_post.map(x => x.split(",").map(ele => ele.trim))
        val rows_relation = lines_relation.map(x => x.split(",").map(ele => ele.trim))

        val pair_post = rows_post.map(x => (x(1), x(0))); //名字，id

        val pair_relation = rows_relation.map(x => (x(0), x)) //MD5,全信息

        val filter_relation_l = rows_relation.filter(x => x(1) == "0") //过滤出head 0
        val pair_relation_l = filter_relation_l.map(x => (x(2), x(0))) //名字，MD5
        val fill_result_l = pair_relation_l.leftOuterJoin(pair_post).map {
            case (key, (a, b)) => (a, b.getOrElse("0")) //md5,id
        }
        val final_l = pair_relation.leftOuterJoin(fill_result_l).map {
            case (md5, (info, id)) =>
                info(1) = id.getOrElse("0")
                (md5, info)
        }
        val filter_relation_r = rows_relation.filter(x => x(3) == "0") //过滤出 tail 0
        val pair_relation_r = filter_relation_r.map(x => (x(4), x(0))) //名字，MD5
        val fill_result_r = pair_relation_r.leftOuterJoin(pair_post).map {
            case (key, (a, b)) => (a, b.getOrElse("0")) //md5,id
        }
        val result = final_l.leftOuterJoin(fill_result_r).map {
            case (md5, (info, id)) =>
                info(3) = id.getOrElse("0")
                info.mkString(",")
        }
        writeToFile(result, dst)
        merge(dst, dst + ".csv")
    }

    def connectedComponents(sc: SparkContext, src: String, dst: String) {

        // Hash function to assign an Id to each article
        def nameHash(title: String): VertexId = {
            title.toLowerCase.replace(" ", "").hashCode.toLong
        }

        val edge_tripl = sc.textFile(src).map { x =>
            val arr = x.split(",").map(e => e.trim)
            ((nameHash(arr(2)), arr(2)), (nameHash(arr(4)), arr(4)), x(5).toLong)
        }
        //val empty_removed = edge_tripl.filter(x => x._1 != "0" && x._2 != "0")
        println("connectedComponents")
        println("edges count : " + edge_tripl.count())

        val edges = edge_tripl.map {
            case (src, dst, w) =>
                Edge(src._1, dst._1, w)
        }

        val vertices = edge_tripl.flatMap {
            case (src, dst, w) =>
                List((src._1, src._2), (dst._1, dst._2))
        }

        println("vertices count:" + vertices.count())

        val g = Graph.fromEdges(edges, "")

        val labled_components = ConnectedComponents.run(g)

        val result = extractEachComponentByVertice(labled_components, vertices)

        val sorted = result.sortBy(x => x._2._2.size)

        val arrayMap = sorted.map {
            case (label, (cw, vw)) =>
                val arr = Array.fill[String](2 + vw.size)("")
                arr(0) = vw.size.toString
                arr(1) = cw.toString
                var i = 2
                for (v <- vw) {
                    arr(i) = v._1 + ":" + v._2
                    i += 1
                }
                arr.mkString(",")
        }
        arrayMap.saveAsTextFile(dst)
        merge(dst, dst + ".txt")

    }

    def extractEachComponentByVertice(labled_components: Graph[Long, Long], vertices: RDD[(VertexId, String)]) = {
        def sendMsg(ctx: EdgeContext[Long, Long, Long]) = {
            ctx.sendToDst(ctx.attr)
            ctx.sendToSrc(ctx.attr)
        }

        val merged_vertice: RDD[(VertexId, (Long, String))] = labled_components.vertices.leftOuterJoin(vertices).map {
            case (id, (label, nameOps)) =>
                (id, (label, nameOps.getOrElse("")))
        }
        //count each vertices's weight
        val vertices_weight: RDD[(VertexId, Long)] = labled_components.aggregateMessages[Long](sendMsg, _ + _)
        val vertices_merge = merged_vertice.leftOuterJoin(vertices_weight).map {
            case (id, ((label, name), vw)) =>
                (label, (name, vw.getOrElse(0L)))
        }

        val group_vertices = vertices_merge.groupByKey()

        //labled_components.persist()

        val group_edges = labled_components.triplets.map(x => (x.srcAttr, x.attr))

        val component_weight = group_edges.reduceByKey((a, b) => (a + b))

        //label:component id
        //cw:total post in component
        //vw:Iterator[(name,numberOfPost)]
        val result = component_weight.leftOuterJoin(group_vertices).map {
            case (label, (cw, vw)) =>
                (label, (cw, vw.getOrElse(Iterator.empty)))
        }

        result
    }

    def extractEachComponentByEdges(labled_components: Graph[Long, Long]) {
        val groups = labled_components.triplets.groupBy {
            case (edgeTriplet) => edgeTriplet.srcAttr
        }
        //print each group
        groups.foreach {
            case (key, iterator_edgeTriplet) =>
                println("label:" + key)

                println("vertices count:")

                var vertices_num = 0;
                if (iterator_edgeTriplet.size == 1)
                    vertices_num = 2;
                else
                    vertices_num = iterator_edgeTriplet.size * 2 - 1;

                println(vertices_num)

                println("edges:")
                for (i <- iterator_edgeTriplet) {
                    print(i.srcId + "-->" + i.dstId + ",")
                }
                println("")

        }
    }

    /**
     * 2562810123,1句实话,1
     */
    def combine_a(arg: RDD[Array[String]]): RDD[String] = {
        val keyValue = arg.map(x => (x(0), (x(1), x(2).toInt)))
        val combined = keyValue.reduceByKey((a, b) => (a._1, a._2 + b._2))
        combined.map(x => Array(x._1, x._2._1, x._2._2.toString).mkString(","))
    }
    /**
     * 02ba52da045f03d1c38d296520733a51,0,1句实话,1364258151,风声水早起,1
     */
    def combine_b(arg: RDD[Array[String]]): RDD[String] = {
        val keyValue = arg.map(x => (x(0), (x(1), x(2), x(3), x(4), x(5).toInt)))
        val combined = keyValue.reduceByKey((a, b) => (a._1, a._2, a._3, a._4, a._5 + b._5))
        combined.map(x => Array(x._1, x._2._1, x._2._2, x._2._3, x._2._4, x._2._5.toString).mkString(","))
    }

    def writeToFile(arg: RDD[String], outputFile: String) {
        arg.saveAsTextFile(outputFile)
    }
    /**
     * 合并文件
     */
    def merge(srcPath: String, dstPath: String): Unit = {
        val hadoopConfig = new Configuration()
        val hdfs = FileSystem.get(hadoopConfig)
        FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
    }
}

