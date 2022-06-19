package research;

import eu.fasten.core.maven.data.ResolvedRevision;
import eu.fasten.core.maven.data.Revision;
import picocli.*;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static research.MergedCallGraphGenerator.*;

public class CallgraphGeneration implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "File with packages to analyse.")
    private File file;

    @CommandLine.Option(names = { "-t", "--transitive"}, description = "Resolve dependents transitively")
    private boolean transitive;

    @CommandLine.Option(names = {"-ci", "--callable-index"}, description = "File path to callable-index folder", required = true)
    protected static String callableIndexUrl;

    @CommandLine.Option(names = {"-tf", "--temporary-folder"}, description = "File path to temporary folder")
    protected static String temporaryFolderUrl;

    @Override
    public Integer call() throws Exception {
        List<String> packages = Files.readAllLines(file.toPath());

        Map<Revision, Set<ResolvedRevision>> packageList = generateDependents(packages, transitive);
        for (Map.Entry<Revision, Set<ResolvedRevision>> entry : packageList.entrySet()) {
            Map<Revision, Set<ResolvedRevision>> dependenciesOfDependents = generateDependenciesForDependents(entry.getValue(), entry.getKey());
            generateAndStoreCallGraphForDependents(dependenciesOfDependents, entry.getKey());
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = new CommandLine(new CallgraphGeneration()).execute(args);
        System.exit(exitCode);
    }

}
