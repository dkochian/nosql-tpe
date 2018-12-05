package ar.edu.itba.nosql;

import org.neo4j.driver.v1.*;

public class Neo4jQueryRunner implements AutoCloseable {

    private static final boolean Q1 = false;

    private static Driver driver;

    public static void main(String[] args) {
        final long start = System.currentTimeMillis();
        driver = GraphDatabase.driver("bolt://node1.it.itba.edu.ar:7689", AuthTokens.basic("jdantur", "jdantur"));

        try ( Session session = driver.session() )
        {
            if(Q1) {
                session.writeTransaction(tx -> {
                    StatementResult result = tx.run("MATCH (s:Stop)\n" +
                            "WITH s.userId as user, MAX(s.tpos) AS length\n" +
                            "RETURN user, length\n;");
                    return result.toString();
                });
                System.out.println("Q1: " + (System.currentTimeMillis() - start) + "ms");
            }
            else {
                session.writeTransaction(tx -> {
                    StatementResult result = tx.run("MATCH (s:Stop)-->(v:Venue)-->()-->(c:Category)\n" +
                            "WHERE c.name = 'Airport'\n" +
                            "WITH COUNT(s) AS amount, s.userId AS user\n" +
                            "RETURN user, amount\n;");
                    return result.toString();
                });
                System.out.println("Q2: " + (System.currentTimeMillis() - start) + "ms");
            }
        }

        System.exit(0);
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }
}
