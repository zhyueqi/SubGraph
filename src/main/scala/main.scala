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

object SubGraph {
    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Simple Application")
        val sc = new SparkContext(conf)

        //去除重复项数据
        //remove_repeating_vertice(sc);
        // remove_repeating_edges(sc);

        //补充 id
        //joinTable(sc);
        //无向图子图
        connectedComponents(sc);
    }

    def remove_repeating_vertice(sc: SparkContext) {
        val inputFile = "file:///Users/zhyueqi/WorkSpace/spark/data/post_test.csv"
        val outputFolder = "file:///Users/zhyueqi/WorkSpace/spark/data/primary_post_test"
        val outputFile = "file:///Users/zhyueqi/WorkSpace/spark/data/single_post_test.csv"
        val lines = sc.textFile(inputFile)
        val lineTrips = lines.map(line => line.split(",").map(elem => elem.trim))
        // lineTrips.persist()
        val combined = combine_a(lineTrips);
        println(combined.count());
        writeToFile(combined, outputFolder);
        merge(outputFolder, outputFile)
    }

    def remove_repeating_edges(sc: SparkContext) {
        val inputFile = "file:///Users/zhyueqi/WorkSpace/spark/data/reRelation_test.csv"
        val outputFolder = "file:///Users/zhyueqi/WorkSpace/spark/data/primary_reRelation_test"
        val outputFile = "file:///Users/zhyueqi/WorkSpace/spark/data/single_reRelation_test.csv"
        val lines = sc.textFile(inputFile)
        val lineTrips = lines.map(line => line.split(",").map(elem => elem.trim))
        // lineTrips.persist()
        val combined = combine_b(lineTrips);
        println(combined.count());
        writeToFile(combined, outputFolder);
        merge(outputFolder, outputFile)
    }

    def joinTable(sc: SparkContext) {
        val post_file = "file:///Users/zhyueqi/WorkSpace/spark/data/single_post_test.csv"
        val relation_file = "file:///Users/zhyueqi/WorkSpace/spark/data/single_reRelation_test.csv"
        val joined_file_folder = "file:///Users/zhyueqi/WorkSpace/spark/data/recover"
        val joined_file_file = "file:///Users/zhyueqi/WorkSpace/spark/data/recover.csv"
        val lines_post = sc.textFile(post_file)
        val lines_relation = sc.textFile(relation_file)

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
        writeToFile(result, joined_file_folder)
        merge(joined_file_folder, joined_file_file)
    }

    def connectedComponents(sc: SparkContext) {
        val joined_file_file = "file:///Users/zhyueqi/WorkSpace/spark/data/recover.csv"

        val edge_tripl = sc.textFile(joined_file_file).map { x =>
            val arr = x.split(",").map(e => e.trim)
            (arr(1), arr(3), x(5).toLong)
        }

        val empty_removed = edge_tripl.filter(x => x._1 != "0" && x._2 != "0")

        println("connectedComponents")
        println("edges count : " + empty_removed.count())

        val edges = empty_removed.map {
            case (src, dst, w) =>
                Edge(src.toLong, dst.toLong, w)
        }

        val g = Graph.fromEdges(edges, "defaultProperty")

        val labled_components = ConnectedComponents.run(g)

        val result = extractEachComponentByVertice(labled_components)

        val file = "file:///Users/zhyueqi/WorkSpace/spark/data/final"
        //FileUtil.fullyDelete(new File(file))

        def countNum(len: Long): Long = {
            if (len == 1)
                2
            else
                len * 2 - 1
        }

        val sorted = result.sortBy {
            x =>
                countNum(x._2._2.size)

        }
        val arrayMap = sorted.map {
            case (label, (cw, vw)) =>
                val arr = Array.fill[String](3 + vw.size)("")
                arr(0) = label.toString
                arr(1) = cw.toString
                arr(2) = countNum(vw.size).toString
                var i = 3
                for (v <- vw) {
                    arr(i) = v.toString
                    i += 1
                }
                arr.mkString(",")
        }
        arrayMap.saveAsTextFile(file)
        merge(file, file + ".csv")

    }

    def extractEachComponentByVertice(labled_components: Graph[Long, Long]) = {
        def sendMsg(ctx: EdgeContext[Long, Long, Long]) = {
            ctx.sendToDst(ctx.attr)
            ctx.sendToSrc(ctx.attr)
        }

        //count each vertices's weight
        val vertices_weight: RDD[(VertexId, Long)] = labled_components.aggregateMessages[Long](sendMsg, _ + _)
        val vertices_merge = labled_components.vertices.leftOuterJoin(vertices_weight).map {
            case (id, (label, vw)) =>
                (label, (id, vw.getOrElse(0L)))
        }

        val group_vertices = vertices_merge.groupByKey()

        //labled_components.persist()

        val group_edges = labled_components.triplets.map(x => (x.srcAttr, x.attr))

        val component_weight = group_edges.reduceByKey((a, b) => (a + b))

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

