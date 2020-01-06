package hu.bme.cs.tegzes.bigdata

import java.io.InputStream
import java.io.PrintWriter
import java.util.*
import java.util.stream.Collectors
import kotlin.math.sqrt
import kotlin.random.Random
import kotlin.streams.toList

private fun getResourcesAsStream(resource: String): InputStream? =
    Rating::class.java.classLoader.getResourceAsStream(resource)

const val featureNum = 20
const val maxEpochs = 10000
const val patience = 10
var learningRate = .0005
const val userRegularizationRate = 0.0001
const val movieRegularizationRate = 0.0001


fun main() {
    val all =
        getResourcesAsStream("ratings.train")!!.bufferedReader().use { reader ->
            reader.lines().map { Rating.parse(it) ?: error("Could not read from $it") }.toList()
        }

    val (aTraining, validation) = all.partition { Random.nextDouble() < 0.9 }
    val training = aTraining.toMutableList()

    val userNum = training.maxBy { it.userId }?.userId?.plus(1) ?: 0
    val movieNum = training.maxBy { it.movieId }?.movieId?.plus(1) ?: 0

    val maxValue = sqrt(1 / (featureNum.toDouble()))
    val userFeatures =
        Array(userNum) { DoubleArray(featureNum) { Random.nextDouble(-maxValue, maxValue) } }
    val movieFeatures =
        Array(movieNum) { DoubleArray(featureNum) { Random.nextDouble(-maxValue, maxValue) } }

    val lastMses: Deque<Double> = ArrayDeque<Double>(10)

    for (epochNum in 1..maxEpochs) {
        training.shuffle()
        val errors = training.parallelStream().map {
            correctFeatures(epochNum, it, userFeatures, movieFeatures)
        }
        val errorStatistics = errors.collect(Collectors.summarizingDouble { it })
        println("\ttraining MSE = ${errorStatistics.average}")
        val validationSummary = validation.parallelStream().map { data ->
            with(data) {
                val guessedRating = userFeatures[userId] dot movieFeatures[movieId]
                val error = rating - guessedRating
                return@map error * error
            }
        }.collect(Collectors.summarizingDouble { it })
        val mse = validationSummary.average
        println("MSE = $mse\tMin = ${validationSummary.min}\tMax=${validationSummary.max}")
        println("*************************************************************** END OF EPOCH $epochNum")
        lastMses.addLast(mse)
        if (lastMses.size >= patience) {
            val oldestMse = lastMses.removeFirst()
            if (lastMses.all { it >= oldestMse }) {
                break
            }
        }
        learningRate *= 0.995
    }

    val questions =
        getResourcesAsStream("ratings.test")!!.bufferedReader().use { reader ->
            reader.lines().map { Question.parse(it) ?: error("Could not read question from '$it'") }.toList()
        }

    PrintWriter("TT_ratings.test").use {
        for (question in questions) {
            val guessedRating = userFeatures[question.userId] dot movieFeatures[question.movieId]
            val guessedGivenStars = Rating.ratingToStars(guessedRating)
            it.println("${question.userId} ${question.movieId} $guessedGivenStars ${question.timestamp}")
        }
    }

    PrintWriter("TT_ratings_raw.test").use {
        for (question in questions) {
            val guessedRating = userFeatures[question.userId] dot movieFeatures[question.movieId]
            val guessedGivenStars = Rating.ratingToRawStars(guessedRating)
            it.println("${question.userId} ${question.movieId} $guessedGivenStars ${question.timestamp}")
        }
    }
}


private fun correctFeatures(
    epochNum: Int,
    knownRating: Rating,
    userFeatures: Array<DoubleArray>,
    movieFeatures: Array<DoubleArray>
): Double {
    with(knownRating) {
        synchronized(userFeatures[userId]) {
            synchronized(movieFeatures[movieId]) {
                val guessedRating = userFeatures[userId] dot movieFeatures[movieId]
                val error = rating - guessedRating
                for (i in 0 until featureNum) {
                    userFeatures[userId][i] += learningRate * (2 * error * movieFeatures[movieId][i] - 2 * userFeatures[userId][i] * userRegularizationRate)
                    movieFeatures[movieId][i] += learningRate * (2 * error * userFeatures[userId][i] - 2 * movieFeatures[movieId][i] * movieRegularizationRate)
                }
                return error * error
            }
        }
    }
}

private infix fun DoubleArray.dot(other: DoubleArray): Double {
    require(this.size == other.size)
    var ret = 0.0
    for (i in indices) {
        ret += this[i] * other[i]
    }
    return ret
}