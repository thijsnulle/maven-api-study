package research;

import com.google.common.collect.BiMap;
import eu.fasten.core.data.DirectedGraph;
import eu.fasten.core.data.callableindex.RocksDao;
import eu.fasten.core.data.opal.MavenCoordinate;
import eu.fasten.core.dbconnectors.PostgresConnector;
import eu.fasten.core.maven.data.ResolvedRevision;
import eu.fasten.core.maven.data.Revision;
import eu.fasten.core.maven.data.Scope;
import eu.fasten.core.maven.resolution.*;
import eu.fasten.core.merge.CGMerger;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import org.jooq.DSLContext;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

public class MergedCallGraphGenerator {

    private static IMavenResolver resolver;
    private static RocksDao dao;

    static {
        try {
            dao = new RocksDao(CallgraphGeneration.callableIndexUrl, true);

            String tempDirLoc = Objects.isNull(CallgraphGeneration.temporaryFolderUrl)
                    ? String.format("%s/tmp", System.getProperty("user.dir"))
                    : CallgraphGeneration.temporaryFolderUrl;
            Files.createDirectories(Paths.get(tempDirLoc));
            resolver = new MavenResolverIO(getDbContext(), new File(tempDirLoc)).loadResolver();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to generate dependents for a list of packages.
     *
     * @param packages List of packages to generate dependents of.
     * @return Map of dependents per package.
     */
    public static Map<Revision, Set<ResolvedRevision>> generateDependents(List<String> packages, boolean transitive) {
        List<Revision> revisions = packages.stream().map(MergedCallGraphGenerator::revisionFromString).collect(Collectors.toList());

        return revisions.stream().collect(Collectors.toMap(
                revision -> revision,
                revision -> {
                    try {
                        // Get configuration for dependent resolution, either transitive or direct based on cli argument.
                        var config = new ResolverConfig().depth(transitive ? ResolverDepth.TRANSITIVE : ResolverDepth.DIRECT);

                        // Resolve dependents for revision, filter unique artefacts and removing testing frameworks.
                        Set<String> uniqueArtefacts = new HashSet<>();
                        var dependents = resolver.resolveDependents(revision, config)
                                .stream()
                                .filter(r -> uniqueArtefacts.add(r.getGroupId()))
                                .filter(r -> List.of("test", "junit", "assertj", "mock").stream().noneMatch(r.getGroupId()::contains))
                                .filter(r -> List.of("test", "junit", "assertj", "mock").stream().noneMatch(r.getArtifactId()::contains))
                                .collect(Collectors.toSet());

                        // Randomly sample a maximum of 100 dependents.
                        var temporaryList = new ArrayList<>(dependents);
                        Collections.shuffle(temporaryList);
                        dependents = new HashSet<>(temporaryList.subList(0, Math.min(100, dependents.size())));

                        // Store dependent information in a separate file.
                        storeDependentsInformation(dependents, revision);
                        return dependents;
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.format("Exception while resolving dependents for %s: %s", revision, e);
                        return new ObjectLinkedOpenHashSet<>();
                    }
                }
        ));
    }

    /**
     * Method to generate dependencies for a set of dependents.
     * NOTE: current revision is being added because of a bug in Fasten.
     *
     * @param dependents        Set of dependents to generate the dependencies of.
     * @param revisionToAnalyse Revision that is being analysed.
     * @return Map of a dependencies list for every dependent.
     */
    public static Map<Revision, Set<ResolvedRevision>> generateDependenciesForDependents(Set<ResolvedRevision> dependents, Revision revisionToAnalyse) {
        return dependents.stream().collect(Collectors.toMap(
                dependent -> dependent,
                dependent -> {
                    try {
                        // // Resolve dependencies for dependent, filter unique artefacts and removing testing frameworks.
                        Set<String> uniqueArtefacts = new HashSet<>();
                        var dependencies = resolver.resolveDependencies(dependent)
                                .stream()
                                .filter(r -> uniqueArtefacts.add(String.format("%s:%s", r.getGroupId(), r.getArtifactId())))
                                .filter(r -> List.of("test", "junit", "assertj", "mock").stream().noneMatch(r.getGroupId()::contains))
                                .filter(r -> List.of("test", "junit", "assertj", "mock").stream().noneMatch(r.getArtifactId()::contains))
                                .collect(Collectors.toSet());

                        // Stop callgraph generation if the revision is not part of the dependencies of the dependent.
                        if (!dependencies.contains(new ResolvedRevision(revisionToAnalyse, Scope.COMPILE))) {
                            return new ObjectLinkedOpenHashSet<>();
                        }

                        // Store dependency information in a separate file.
                        storeDependenciesInformation(dependencies, revisionToAnalyse, dependent);
                        return dependencies;
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.format("Exception while resolving dependencies for %s as dependent of %s.", dependent, revisionToAnalyse);
                        return new ObjectLinkedOpenHashSet<>();
                    }
                })
        );
    }

    /**
     * Method to generate and store callgraphs for a map of dependents.
     *  @param dependents        Map representing the dependencies of a dependent.
     * @param revisionToAnalyse Revision that is being analysed, only used for generating output filenames.
     */
    public static void generateAndStoreCallGraphForDependents(Map<Revision, Set<ResolvedRevision>> dependents, Revision revisionToAnalyse) {
        dependents.forEach(
                (dependent, dependencies) -> {
                    try {
                        // Extract the list of Maven coordinates to create the callgraph from.
                        List<String> coords = dependencies.stream().map(d -> String.format("%s:%s:%s", d.getGroupId(), d.getArtifactId(), d.version))
                                .collect(Collectors.toList());
                        coords.add(String.format("%s:%s:%s", dependent.getGroupId(), dependent.getArtifactId(), dependent.version));

                        // Create a single callgraph of a dependent and its dependencies.
                        CGMerger merger = new CGMerger(coords, getDbContext(), dao);
                        DirectedGraph callgraph = merger.mergeAllDeps();
                        BiMap<Long, String> uris = merger.getAllUrisFromDB(callgraph);

                        // Extract callgraph and URI information and store into separate files.
                        Path cgPath = Paths.get(String.format("input/%s_%s_%s/%s_%s_%s/callgraph.csv", revisionToAnalyse.getGroupId(), revisionToAnalyse.getArtifactId(), revisionToAnalyse.version, dependent.getGroupId(), dependent.getArtifactId(), dependent.version));
                        Path uriPath = Paths.get(String.format("input/%s_%s_%s/%s_%s_%s/uris.csv", revisionToAnalyse.getGroupId(), revisionToAnalyse.getArtifactId(), revisionToAnalyse.version, dependent.getGroupId(), dependent.getArtifactId(), dependent.version));

                        List<String> cgData = callgraph.edgeSet().stream().map(x -> x.firstLong() + "," + x.secondLong()).collect(Collectors.toList());
                        List<String> uriData = uris.entrySet().stream().map(e -> String.format("%s,\"%s\"", e.getKey(), e.getValue())).collect(Collectors.toList());
                        cgData.add(0, "source,target");
                        uriData.add(0, "id,uri");

                        writeToFile(cgPath, cgData);
                        writeToFile(uriPath, uriData);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
    }

    /**
     * Method to store all the generated information concercing dependents.
     * @param dependents Dependents to be stored.
     * @param r          Package to analyse.
     */
    private static void storeDependentsInformation(Set<ResolvedRevision> dependents, Revision r) {
        Path path = Paths.get(String.format("input/%s_%s_%s/dependents.txt", r.getGroupId(), r.getArtifactId(), r.version));
        List<String> data = dependents.stream().map(x -> String.format("%s_%s_%s", x.getGroupId(), x.getArtifactId(), x.version)).collect(Collectors.toList());
        writeToFile(path, data);
    }

    /**
     * Method to store all the generated information concercing dependencies of a dependent.
     * @param dependencies Dependencies to be stored.
     * @param r            Package to analyse.
     * @param d            Dependent of which the callgraph is generated.
     */
    private static void storeDependenciesInformation(Set<ResolvedRevision> dependencies, Revision r, Revision d) {
        Path path = Paths.get(String.format("input/%s_%s_%s/%s_%s_%s/dependencies.txt", r.getGroupId(), r.getArtifactId(), r.version, d.getGroupId(), d.getArtifactId(), d.version));
        List<String> data = dependencies.stream().map(x -> String.format("%s_%s_%s", x.getGroupId(), x.getArtifactId(), x.version)).collect(Collectors.toList());
        writeToFile(path, data);
    }

    /**
     * Method to write a list of strings to a file.
     *
     * @param path Path to the file.
     * @param data Data to write to the file.
     */
    private static void writeToFile(Path path, List<String> data) {
        try {
            Files.createDirectories(path.getParent());
            Files.write(path, data, StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to generate a `Revision` object from a package in string format.
     *
     * @param pkg Package in strong format.
     * @return `Revision` object of the package.
     */
    private static Revision revisionFromString(String pkg) {
        MavenCoordinate coordinate = MavenCoordinate.fromString(pkg, null);
        return new Revision(coordinate.getGroupID(), coordinate.getArtifactID(), coordinate.getVersionConstraint(), new Timestamp(-1));
    }

    /**
     * Method to get the current database context.
     *
     * @return Database context, if found.
     */
    private static DSLContext getDbContext() throws Exception {
        return PostgresConnector.getDSLContext("jdbc:postgresql://host.docker.internal:5432/fasten_java", "fasten", false);
    }

}
