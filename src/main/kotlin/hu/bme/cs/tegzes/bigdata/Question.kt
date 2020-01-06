package hu.bme.cs.tegzes.bigdata

import java.util.regex.Pattern

data class Question(val userId: Int, val movieId: Int, val timestamp: String) {
    companion object {
        fun parse(line: String): Question? {
            val parts = line.split(Pattern.compile("\\s+"))
            if (parts.size != 4) {
                return null
            }
            val userId = parts[0].toIntOrNull() ?: return null
            val movieId = parts[1].toIntOrNull() ?: return null
            val timestamp = parts[3]

            return Question(userId, movieId, timestamp)
        }
    }
}