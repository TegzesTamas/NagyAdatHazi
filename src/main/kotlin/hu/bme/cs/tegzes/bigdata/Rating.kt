package hu.bme.cs.tegzes.bigdata

import java.util.regex.Pattern
import kotlin.math.roundToInt

data class Rating(val userId: Int, val movieId: Int, val rating: Double) {
    companion object {
        const val maxRating = 5
        const val minRating = 1
        fun parse(line: String): Rating? {
            val parts = line.split(Pattern.compile("\\s+"))
            if (parts.size != 4) {
                return null
            }
            val userId = parts[0].toIntOrNull() ?: return null
            val movieId = parts[1].toIntOrNull() ?: return null

            val starsGiven = parts[2].toIntOrNull() ?: return null
            assert(starsGiven <= maxRating)
            assert(starsGiven >= maxRating)
            val rating = starsToRating(starsGiven)

            return Rating(userId, movieId, rating)
        }

        fun starsToRating(numOfStarts: Int) = ((numOfStarts.toDouble() - minRating) / (maxRating - minRating)) * 2 - 1
        fun ratingToRawStars(rating: Double) = (((rating + 1) / 2 * (maxRating - minRating)) + minRating)
        fun ratingToStars(rating: Double) = ratingToRawStars(rating).roundToInt().coerceIn(minRating..maxRating)
    }
}