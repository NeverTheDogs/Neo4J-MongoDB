import java.io.PrintWriter;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;


public class Neo4j implements AutoCloseable
{
    private final Driver driver;

    public Neo4j( String uri, String user, String password )
    {
        driver = GraphDatabase.driver( uri, AuthTokens.basic( "neo4j", "basi2" ) );
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

    public void create()
    {
        try ( Session session = driver.session() )
        {
            session.writeTransaction((Transaction tx) -> {
                tx.run( "LOAD CSV WITH HEADERS FROM 'file:///aziende.csv' AS Line FIELDTERMINATOR ';' \n" +
                        "CREATE (C:Company { Siren: toInteger(Line.Siren), Denomination:Line.Denomination, Secteur: Line.Secteur, Geoloc:Line.Geoloc , Date:Line.Date,"
                        + " Greffe:Line.Greffe, FicheEntreprise:Line.Fiche })\n" +
                        "MERGE (FJ:FormeJuridique {name: coalesce(Line.FormeJuridique, \"Unknown\")})\n" +
                        "MERGE (APE:CodeAPE {name: coalesce(Line.CodeAPE, \"Unknown\")})\n" +
                        "MERGE (ADR:Adresse {name: coalesce(Line.Adresse, \"Unknown\")})\n" +
                        "MERGE (CP:Codepostal {name: coalesce(Line.Codepostal, \"Unknown\")})\n" +
                        "MERGE (V:Ville {name: coalesce(Line.Ville, \"Unknown\")})\n" +
                        "MERGE (R:Region {name: coalesce(Line.Region, \"Unknown\")})\n" +
                        "MERGE (C)-[:HAS_FORME]->(FJ)\n" +
                        "MERGE (C)-[:HAS_CODEAPE]->(APE)\n" +
                        "MERGE (C)-[:HAS_ADRESSE]->(ADR)\n" +
                        "MERGE (ADR)-[:HAS_CODEPOSTAL]->(CP)\n" +
                        "MERGE (CP)-[:HAS_CITY]->(V)\n" +
                        "MERGE (V)-[:HAS_REGION]->(R)");
                return null;
            });
        }
    }
    
    public void delete()
    {
        try ( Session session = driver.session() )
        {
            session.writeTransaction((Transaction tx) -> {
                tx.run( "MATCH (n) DETACH DELETE n");
                return null;
            });
        }
    }
    
    public void indexes()
    {
        try ( Session session = driver.session() )
        {
            session.writeTransaction((Transaction tx) -> {
                tx.run( "CREATE INDEX ON :Company(Siren)");
                tx.run( "CREATE INDEX ON :FormeJuridique(name)");
                tx.run( "CREATE INDEX ON :CodeAPE(name)");
                tx.run( "CREATE INDEX ON :Adresse(name)");
                tx.run( "CREATE INDEX ON :Codepostal(name)");
                tx.run( "CREATE INDEX ON :Ville(name)");
                tx.run( "CREATE INDEX ON :Region(name)");          
                return null;
            });
        }
    }
    
    public void clearUnk()
    {
        try ( Session session = driver.session() )
        {
            session.writeTransaction((Transaction tx) -> {
                tx.run( "MATCH (n { name: 'Unknown' }) DETACH DELETE n");
                return null;
            });
        }
    }        
    
    public void query(int a)
    {
        try ( Session session = driver.session() )
        {    
            
            session.writeTransaction((Transaction tx) -> {
                //tx.run("MATCH (n) return count(n)");                
               if((a>=0)&&(a<31)) {tx.run( "MATCH (c:Company) WHERE c.Greffe=('LYON') return count(c)");}              
               else if((a>=31)&&(a<62)){tx.run( "MATCH (c:Company)-[:HAS_FORME]->(fj:FormeJuridique) WHERE (fj.name=('Société à responsabilité limitée') AND c.Denomination=~('A.*') AND (c.Greffe<=('LYON')OR c.Greffe<=('NANTES'))) return count(c)");}
               else if((a>=62)&&(a<93)){tx.run( "MATCH (c:Company)-[:HAS_FORME]->(fj:FormeJuridique) WHERE (fj.name=('Société à responsabilité limitée') AND c.Denomination=~('A.*') AND (c.Siren>=803000000 AND c.Siren<804000000)) return count(c)");}
               else if((a>=93)&&(a<124)){tx.run( "MATCH (c:Company)-[:HAS_FORME]->(fj:FormeJuridique) WHERE (fj.name=('Société à responsabilité limitée') AND c.Denomination=~('A.*') AND (c.Siren>=803000000 AND c.Siren<804000000)) AND c.Date=~('2014.*') return count(c)");}
               else if((a>=124)&&(a<155)){tx.run( "MATCH (c:Company)-[:HAS_FORME]->(fj:FormeJuridique), (c)-[:HAS_CODEAPE]->(ape:CodeAPE) WHERE (fj.name=('Société à responsabilité limitée') AND c.Denomination=~('B.*') AND (c.Siren>=803000000 AND c.Siren<804000000) AND c.Date=~('2014.*') AND ape.name=~('.*Z')) return count(c)");}
               else if((a>=155)&&(a<186)){tx.run( "MATCH (c:Company)-[:HAS_FORME]->(fj:FormeJuridique), (c)-[:HAS_CODEAPE]->(ape:CodeAPE) WHERE (fj.name=('Société à responsabilité limitée') AND c.Denomination=~('B.*') AND (c.Siren>=803000000 AND c.Siren<804000000) AND c.Date=~('2014.*') AND ape.name=~('0.*')) return count(c)");}
               else if((a>=186)&&(a<217)){tx.run( "MATCH (c:Company)-[:HAS_FORME]->(fj:FormeJuridique), (c)-[:HAS_ADRESSE]->(a:Adresse) WHERE (fj.name=('Société à responsabilité limitée') AND c.Denomination=~('B.*') AND (c.Siren>=803000000 AND c.Siren<804000000) AND c.Date=~('2014.*') AND a.name=~('1.*')) return count(c)");}
                
                return null;
            });
        }
    }                
    
    public static void main( String... args ) throws Exception
    {
        try ( Neo4j exec = new Neo4j( "bolt://localhost:7687", "neo4j", "password" ) )
        {
            System.out.println("Si parte");
            
            /*System.out.println("Creazione indici");
            exeQuery.indexes();
            System.out.println("Creazione DB");
            long creOn = System.currentTimeMillis();
            exeQuery.create();ads
            long creOff = System.currentTimeMillis();
            System.out.println("Fine prima inizio seconda");
            long creTime = creOff - creOn;
            System.out.println("La query di inserimento è stata eseguita in " +creTime+ "ms");
            //long delOn = System.currentTimeMillis();
            //exeQuery.delete();
            //long delOff = System.currentTimeMillis();
            //long delTime = delOff - delOn;
            //System.out.println("La query di Cancellamento è stata eseguita in " +delTime+ "ms");
            exeQuery.clearUnk();*/ 
           
        
          
                
            long tot=0;
            long selTime=0;   
            int a=0;
            PrintWriter writer = new PrintWriter("/Users/Cristian/Desktop/time_query.txt", "UTF-8"); 
            for(int i=0;i<=217;i++)
            {                                                           
                    if ((i==0)||(i==31)||(i==62)||(i==93)||(i==124)||(i==155)||(i==186))
                    {
                        a=a+1;
                        writer.println("Query n°"+a);
                        exec.query(i);
                    }
                    else if((i>0)&&(i<31)) 
                    {                        
                        long timOn = System.currentTimeMillis();
                        exec.query(i);
                        long timOff = System.currentTimeMillis();
                        selTime = timOff - timOn;
                        writer.println(selTime);
                        //System.out.println("La query è stata eseguita in " +selTime+ "ms");                        
                        tot=tot+selTime;                       
                    }
                    else if((i>31)&&(i<62)) 
                    {                        
                        long timOn = System.currentTimeMillis();
                        exec.query(i);
                        long timOff = System.currentTimeMillis();
                        selTime = timOff - timOn;
                        writer.println(selTime);
                        //System.out.println("La query è stata eseguita in " +selTime+ "ms");                        
                        tot=tot+selTime;                         
                    }
                    else if((i>62)&&(i<93)) 
                    {                        
                        long timOn = System.currentTimeMillis();
                        exec.query(i);
                        long timOff = System.currentTimeMillis();
                        selTime = timOff - timOn;
                        writer.println(selTime);
                        //System.out.println("La query è stata eseguita in " +selTime+ "ms");                        
                        tot=tot+selTime;                         
                    }
                    else if((i>93)&&(i<124)) 
                    {                        
                        long timOn = System.currentTimeMillis();
                        exec.query(i);
                        long timOff = System.currentTimeMillis();
                        selTime = timOff - timOn;
                        writer.println(selTime);
                        //System.out.println("La query è stata eseguita in " +selTime+ "ms");                        
                        tot=tot+selTime;                         
                    }
                    else if((i>124)&&(i<155)) 
                    {                        
                        long timOn = System.currentTimeMillis();
                        exec.query(i);
                        long timOff = System.currentTimeMillis();
                        selTime = timOff - timOn;
                        writer.println(selTime);
                        //System.out.println("La query è stata eseguita in " +selTime+ "ms");                        
                        tot=tot+selTime;                         
                    }
                    else if((i>155)&&(i<186)) 
                    {                        
                        long timOn = System.currentTimeMillis();
                        exec.query(i);
                        long timOff = System.currentTimeMillis();
                        selTime = timOff - timOn;
                        writer.println(selTime);
                        //System.out.println("La query è stata eseguita in " +selTime+ "ms");                        
                        tot=tot+selTime;                         
                    }
                    else if((i>186)&&(i<217)) 
                    {                        
                        long timOn = System.currentTimeMillis();
                        exec.query(i);
                        long timOff = System.currentTimeMillis();
                        selTime = timOff - timOn;
                        writer.println(selTime);
                        //System.out.println("La query è stata eseguita in " +selTime+ "ms");                        
                        tot=tot+selTime;                         
                    }
                
                //writer.println("Prova "+i+" = "+selTime+" ms");
                //System.out.println("Fine esecuzione");
                
                //System.out.println("Le query sono state eseguite su "+ a[j]+" dati ");               
            }
            
             //System.out.println("Il tempo medio è di "+ tot/30 + "ms");
            writer.close();
        }
    }
}



