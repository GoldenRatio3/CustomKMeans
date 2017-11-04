import scala.xml.XML
import scala.xml.Elem
import scala.io.Source
import scala.math.pow
import scala.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.util.random.XORShiftRandom

class User(var id:Int, var upVotes:Int, var downVotes:Int, var reputation:Int, var views:Int, var valid:Boolean, var cluster: Int, var clusterChanged: Boolean) extends Serializable  {
	
	def this(inId: Int, inUpVotes: Int, inDownVotes : Int, inReputation : Int, inViews : Int, inValid : Boolean) {
		this(inId, inUpVotes, inDownVotes, inReputation, inViews, inValid, -1, true)
	}

	def this(elem: Elem) {

		this (0, 0, 0, 0, 0, true)

		try {
			this.id =  (elem \ "@AccountId").text.toInt
			this.upVotes = (elem \ "@UpVotes").text.toInt
			this.downVotes = (elem \ "@DownVotes").text.toInt
			this.reputation = (elem \ "@Reputation").text.toInt
			this.views = (elem \ "@Views").text.toInt

		} catch {
			// Mark as invalid if you cannot build from xml
			case e:Exception=>this.valid = false
		}
	}
	def findDistance(centroid : User) : Double = {
		math.sqrt(math.pow((upVotes - centroid.upVotes), 2) + math.pow((downVotes - centroid.downVotes), 2) + math.pow((reputation - centroid.reputation), 2) + math.pow((views - centroid.views), 2))
	}

	//Find the euclidean distance from an array of centroids and changes centroid index to closest
	def findClosest(centroids : Array[User]) : User = {
		clusterChanged = false

		//Get distance to each centroid
		var distances = centroids.map(findDistance)

		//Find the lowest
		var i = 1
		var lowestIndex = 0
		var lowestValue = distances(0)
		for (i <- (1 to distances.length - 1)) {
			if (distances(i) < lowestValue) {
				lowestValue = distances(i)
				lowestIndex = i
			}
		}

		// Determine if the assigned cluster has changed
		if (lowestIndex != cluster) {
			cluster = lowestIndex
			clusterChanged = true
		}

		return this
	}
}

//Load data
//Only rows that start with "<row"
//Remove first valid row because it is a bot which massively skews results
// -->Experiment Testing: var users = sc.parallelize(sc.textFile("hdfs://moonshot-ha-nameservice/data/stackoverflow/Users").take(100)).filter(line => line.startsWith("  <row")).mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter).map(line => XML.loadString(line)).map(elem => new User(elem)).filter(user => user.valid).persist()
var users = sc.textFile("hdfs://moonshot-ha-nameservice/data/stackoverflow/Users").filter(line => line.startsWith("  <row")).mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter).map(line => XML.loadString(line)).map(elem => new User(elem)).filter(user => user.valid).persist()

//the all important k
val k : Int = 4

//initialize centroids randomly from users
val centroidBuffer = scala.collection.mutable.ArrayBuffer.empty[User]
for (i <- (0 to k - 1)) {
	var sample : User = new User(0, 0, 0, 0, 0, true)
	//Ensure each centroid is unique
	do {
		sample = users.takeSample(false, 1)(0)
	} while (centroidBuffer.count(user => user.upVotes == sample.upVotes && user.downVotes == sample.downVotes && user.reputation == sample.reputation && user.views == sample.views) != 0)
	centroidBuffer += new User(0, sample.upVotes, sample.downVotes, sample.reputation, sample.views, true)
}

val centroids : Array[User] = centroidBuffer.toArray

var iteration = 1
do {
	println("Iteration " + iteration)
	iteration = iteration + 1

	//Reallocate users to each centroid
	users = users.map(user => user.findClosest(centroids)).persist()

	//Redetermine average for each centroid
	for (i <- (0 to centroids.length - 1)) {
		try {
			val centroidUsers = users.filter(user => user.cluster == i).persist()
			val centroidUsersCount = centroidUsers.count()
			centroids(i).upVotes = (centroidUsers.map(user => user.upVotes).reduce(_ + _) / centroidUsersCount.toLong).toInt
			centroids(i).downVotes = (centroidUsers.map(user => user.downVotes).reduce(_ + _) / centroidUsersCount.toLong).toInt
			centroids(i).reputation = (centroidUsers.map(user => user.reputation).reduce(_ + _) / centroidUsersCount.toLong).toInt
			centroids(i).views = (centroidUsers.map(user => user.views).reduce(_ + _) / centroidUsersCount.toLong).toInt
		} catch {
			case e:Exception => println("WARN: Cluster " + i + " is empty") //this shouldnt happen
		}
		println("Centroid " + i + ".")
		println("U " + centroids(i).upVotes + " D " + centroids(i).downVotes + " R " + centroids(i).reputation + " V " + centroids(i).views)
	}

	println()
} while (users.filter(user => user.clusterChanged).count() != 0) // do this until no reallocation take place

println()
println("Results:")

for (i <- (0 to centroids.length - 1)) {
	//count number of users in each centroid
	val centroidUsers = users.filter(user => user.cluster == i)
	val centroidUsersCount = centroidUsers.count()
	println("Centroid " + i + ".")
	println("Total " + centroidUsersCount)
	println("U " + centroids(i).upVotes + " D " + centroids(i).downVotes + " R " + centroids(i).reputation + " V " + centroids(i).views)
	println()
}