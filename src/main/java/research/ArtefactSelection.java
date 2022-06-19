package research;

import eu.fasten.core.data.metadatadb.codegen.tables.PackageVersions;
import eu.fasten.core.data.metadatadb.codegen.tables.Packages;
import eu.fasten.core.dbconnectors.PostgresConnector;
import eu.fasten.core.maven.data.Revision;
import eu.fasten.core.maven.resolution.*;
import org.apache.commons.math3.util.Pair;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.jooq.DSLContext;
import org.jooq.Record4;
import picocli.CommandLine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class ArtefactSelection implements Callable<Integer> {

    @CommandLine.Option(names = {"-sd", "--start-date"}, description = "Start date of sampling frame in format dd/MM/yyyy", required = true)
    private static String startDate;

    @CommandLine.Option(names = {"-ed", "--end-date"}, description = "End date of sampling frame in format dd/MM/yyyy", required = true)
    private static String endDate;

    @CommandLine.Option(names = {"-tf", "--temporary-folder"}, description = "File path to temporary folder")
    protected static String temporaryFolderUrl;

    private static IMavenResolver resolver;
    private static DSLContext context;

    static {
        try {
            context = PostgresConnector.getDSLContext("jdbc:postgresql://localhost:5432/fasten_java", "fasten", false);

            String tempDirLoc = Objects.isNull(CallgraphGeneration.temporaryFolderUrl)
                    ? String.format("%s/tmp", System.getProperty("user.dir"))
                    : CallgraphGeneration.temporaryFolderUrl;
            Files.createDirectories(Paths.get(tempDirLoc));
            resolver = new MavenResolverIO(context, new File(tempDirLoc)).loadResolver();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Integer call() throws Exception {
        // 1. Select packages from 1st of October to 31st of March.
        Timestamp begin = Timestamp.from(new SimpleDateFormat("dd/MM/yyyy").parse(startDate).toInstant());
        Timestamp end = Timestamp.from(new SimpleDateFormat("dd/MM/yyyy").parse(endDate).toInstant());

        var result = context.select(PackageVersions.PACKAGE_VERSIONS.ID, Packages.PACKAGES.PACKAGE_NAME, PackageVersions.PACKAGE_VERSIONS.VERSION, PackageVersions.PACKAGE_VERSIONS.CREATED_AT)
                .from(Packages.PACKAGES, PackageVersions.PACKAGE_VERSIONS)
                .where(Packages.PACKAGES.ID.eq(PackageVersions.PACKAGE_VERSIONS.PACKAGE_ID))
                .and(PackageVersions.PACKAGE_VERSIONS.CREATED_AT.greaterOrEqual(begin))
                .and(PackageVersions.PACKAGE_VERSIONS.CREATED_AT.lessOrEqual(end))
                .fetch();
        System.out.println("Retrieved " + result.size() + " artefacts from the database.");

        // 2. Sort packages based on oldest timestamp first, remove duplicate packages.
        Set<String> packages = new HashSet<>();
        result.sort(Comparator.comparing(Record4::value4));
        var sampleResult = result.stream()
                .filter(r -> List.of("test", "junit", "mock", "assertj").stream().noneMatch(r.value2()::contains))
                .collect(Collectors.toList());
        System.out.println("Filtered " + (result.size() - sampleResult.size()) + " testing artefacts.");

        var newResult = sampleResult.stream()
                .filter(r -> packages.add(r.value2().split(":")[0]))
                .map(r -> new Revision(r.value1(), r.value2().split(":")[0], r.value2().split(":")[1], r.value3(), r.value4()))
                .collect(Collectors.toList());
        System.out.println("Filtered " + (newResult.size() - sampleResult.size()) + " non-unique artefacts.");

        // 3. Calculate the number of dependents for every package.
        ResolverConfig config = ResolverConfig.resolve().depth(ResolverDepth.TRANSITIVE).alwaysIncludeProvided(false);
        List<Pair<Revision, Double>> dependentInformation = newResult.stream().map(r -> {
            try {
                HashSet<String> uniqueDependents = new HashSet<>();
                var dependents = resolver.resolveDependents(r, config).stream()
                        .filter(_r -> uniqueDependents.add(_r.getGroupId()))
                        .collect(Collectors.toList());

                return Pair.create(r, (double) dependents.size());
            } catch (Exception e) {
                return Pair.create(r, 0.0);
            }
        }).collect(Collectors.toList());

        // 4. Store dependent counts.
        try (PrintWriter out = new PrintWriter("dependents-count.txt")) {
            dependentInformation.stream()
                    .sorted(Comparator.comparing(Pair::getSecond))
                    .forEach(p -> out.println(String.format("%s:%s:%s,%d", p.getFirst().getGroupId(), p.getFirst().getArtifactId(), p.getFirst().version, p.getSecond().intValue())));
        }

        // 5. Randomly sample from the dataset, where the weight is the number of dependents.
        EnumeratedDistribution<Revision> distribution = new EnumeratedDistribution<>(dependentInformation);

        // 6. Compute this for all confidence intervals and store in .txt files.
        generateSampleSet(distribution, 95, 5, 384);  // 95% confidence, 5% margin of error.
        generateSampleSet(distribution, 95, 3, 1066); // 95% confidence, 3% margin of error.
        generateSampleSet(distribution, 99, 5, 660);  // 99% confidence, 5% margin of error.
        generateSampleSet(distribution, 99, 3, 1831); // 99% confidence, 3% margin of error.

        return 0;
    }

    private static void generateSampleSet(EnumeratedDistribution<Revision> distribution, int conf, int error, int sampleSize) throws FileNotFoundException {
        Set<Revision> sampleSet = new HashSet<>();
        if (distribution.getPmf().size() < sampleSize) {
            System.out.println("Not enough artefacts available for sample size: " + sampleSize);
            return;
        }

        while (sampleSet.size() < sampleSize) {
            sampleSet.add(distribution.sample());
        }

        String output = sampleSet.stream().map(r -> String.format("%s:%s:%s", r.getGroupId(), r.getArtifactId(), r.version)).collect(Collectors.joining("\n"));
        try (PrintWriter out = new PrintWriter(String.format("artefacts_to_analyse-%d-%d.txt", conf, error))) {
            out.println(output);
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = new CommandLine(new ArtefactSelection()).execute(args);
        System.exit(exitCode);
    }

}