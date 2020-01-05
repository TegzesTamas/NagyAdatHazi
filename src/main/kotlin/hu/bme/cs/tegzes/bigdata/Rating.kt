package hu.bme.cs.tegzes.bigdata

import java.util.regex.Pattern

data class Rating(val userId: Int, val movieId: Int, val rating: Double) {
    companion object {
        const val maxRating = 5
        fun parse(line: String): Rating? {
            val parts = line.split(Pattern.compile("\\s+"))
            if (parts.size != 4) {
                return null
            }
            val userId = parts[0].toIntOrNull() ?: return null
            val movieId = parts[1].toIntOrNull() ?: return null

            val starsGiven = parts[2].toIntOrNull() ?: return null
            assert(starsGiven <= maxRating)
            val rating = starsGiven.toDouble() / maxRating

            return Rating(userId, movieId, rating)
        }
    }
}