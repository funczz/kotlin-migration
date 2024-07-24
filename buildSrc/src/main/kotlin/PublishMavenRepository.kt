object PublishMavenRepository {

    fun url(version: String, baseUrl: String): String {
        return when {
            version.endsWith("-SNAPSHOT") -> {
                "$baseUrl/snapshots"
            }

            version.contains("""-dev\.\d+\.uncommitted\+.+""".toRegex()) -> {
                "$baseUrl/uncommitted"
            }

            version.contains("""-dev\.""".toRegex()) -> {
                "$baseUrl/unstable"
            }

            version.contains("""-rc\.""".toRegex()) -> {
                "$baseUrl/testing"
            }

            else -> "$baseUrl/stable"
        }
    }
}