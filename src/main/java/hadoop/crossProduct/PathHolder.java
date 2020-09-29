package hadoop.crossProduct;


import org.apache.hadoop.fs.Path;

class PathHolder {
    private final String[] args;

    PathHolder(String[] args) {
        this.args = args;
    }

    private boolean containsArgs() {
        return args.length == 4;
    }

    Path outputPath() {
        return pathOf("/Users/koushiken/Desktop/boundOutput/");
    }

    Path mfbbFilePath() {
        if (containsArgs())
            return pathOf(args[0]);
        return pathOf("/Users/koushiken/Desktop/Sample_data/mf_bb.csv");
    }

    Path mfbsFilePath() {
        if (containsArgs())
            return pathOf(args[1]);
        return pathOf("/Users/koushiken/Desktop/Sample_data/mf_bs.csv");
    }

    Path qaScopeFilePath() {
        if (containsArgs())
            return pathOf(args[2]);
        return pathOf("/Users/koushiken/Desktop/Sample_data/qa_scope.csv");
    }

    Path qaGaugeFilePath() {
        if (containsArgs())
            return pathOf(args[3]);
        return pathOf("/Users/koushiken/Desktop/Sample_data/qa_gauge.csv");
    }

    private static Path pathOf(String dir) {
        return new Path(dir);
    }
}
