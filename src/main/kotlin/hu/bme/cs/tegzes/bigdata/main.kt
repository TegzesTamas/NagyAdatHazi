package hu.bme.cs.tegzes.bigdata

import java.io.InputStream
import java.util.stream.Collectors
import kotlin.math.sqrt
import kotlin.random.Random
import kotlin.streams.toList

private fun getResourcesAsStream(resource: String): InputStream? =
    Rating::class.java.classLoader.getResourceAsStream(resource)

const val featureNum = 100
const val epochs = 100
const val steps = 100
const val learningRate = .0001


fun main() {
    val all =
        getResourcesAsStream("ratings.train")!!.bufferedReader().use { reader ->
            reader.lines().map { Rating.parse(it) ?: error("Could not read from $it") }.toList()
        }

    val (training, validation) = all.partition { Random.nextDouble() < 0.9 }

    val maxRating = training.maxBy { it.rating }?.rating ?: 0
    print(maxRating)
    val userNum = training.maxBy { it.userId }?.userId?.plus(1) ?: 0
    val movieNum = training.maxBy { it.movieId }?.movieId?.plus(1) ?: 0

    val userFeatures =
        Array(userNum) { DoubleArray(featureNum) { Random.nextDouble(sqrt(1 / (featureNum.toDouble()))) } }
    val movieFeatures =
        Array(movieNum) { DoubleArray(featureNum) { Random.nextDouble(sqrt(1 / (featureNum.toDouble()))) } }
    val newUserFeatures = Array(userNum) { userFeatures[it].clone() }
    val newMovieFeatures = Array(movieNum) { movieFeatures[it].clone() }

    var lastMse = Double.MAX_VALUE

    for (epochNum in 1..epochs) {
        repeat(steps) { stepNum ->
            println("******** EPOCH #$epochNum, STEP #$stepNum ********")
            val startTime = System.currentTimeMillis()
            for (i in userFeatures.indices) {
                for (j in 0 until featureNum) {
                    userFeatures[i][j] = newUserFeatures[i][j]
                }
            }
            for (i in movieFeatures.indices) {
                for (j in 0 until featureNum) {
                    movieFeatures[i][j] = newMovieFeatures[i][j]
                }
            }
            val copyTime = System.currentTimeMillis()
//            println("Cop: ${(copyTime - startTime) / 1000.0} s")
            val errors = training.parallelStream().map {
                correctFeatures(it, userFeatures, movieFeatures, newUserFeatures, newMovieFeatures)
            }
            val parTime = System.currentTimeMillis()
//            println("Sub: ${(parTime - copyTime) / 1000.0} s")
            val errorStatistics = errors.collect(Collectors.summarizingDouble { it })
            val sumTime = System.currentTimeMillis()
            println("MSE = ${errorStatistics.average}")
//            println("Min = ${errorStatistics.min}")
//            println("Max = ${errorStatistics.max}")
//            println("Sum: ${(sumTime - parTime) / 1000.0} s")
//            println("TOT = ${(sumTime - startTime) / 1000.0} s")
        }
        val validationSummary = validation.parallelStream().map { data ->
            with(data) {
                val guessedRating = userFeatures[userId] dot movieFeatures[movieId]
                val error = rating - guessedRating
                return@map error * error
            }
        }.collect(Collectors.summarizingDouble { it })
        println("************************************************************************************************ END OF EPOCH $epochNum")
        val mse = validationSummary.average
        println("MSE = $mse\tMin = ${validationSummary.min}\tMax=${validationSummary.max}")
        println("************************************************************************************************ END OF EPOCH $epochNum")
        if (lastMse - mse < 0.00001) {
            break
        }
        lastMse = mse
    }
    println(newMovieFeatures)
    println()
    println(newUserFeatures)
}


private fun correctFeatures(
    knownRating: Rating,
    userFeatures: Array<DoubleArray>,
    movieFeatures: Array<DoubleArray>,
    newUserFeatures: Array<DoubleArray>,
    newMovieFeatures: Array<DoubleArray>
): Double {
    with(knownRating) {
        val guessedRating = userFeatures[userId] dot movieFeatures[movieId]
        val error = rating - guessedRating
        for (i in 0 until featureNum) {
            synchronized(newUserFeatures[userId]) {
                //            run {
                newUserFeatures[userId][i] += learningRate * 2 * error * movieFeatures[movieId][i]
            }
            synchronized(newMovieFeatures[movieId]) {
                //            run {
                newMovieFeatures[movieId][i] += learningRate * 2 * error * userFeatures[userId][i]
            }
        }
        return error * error
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