public class Driver {
    public static void main(String[] args) throws Exception {

        /*
            args0: original dataset
            args1: output directory for DividerByUser job
            args2: output directory for Co_occurenceMatrixGenerator job
            args3: output directory for AverageScoreEachUser job
            args4: output directory for MovieUserAverage job
            args5: output directory for NormalizeCo_occurrenceMatrix job
            args6: output directory for Multiplication job
            args7: output directory for MultiplicationSum job
            args8: output directory for RecommenderListGenerator job
        */

        DataDividerByUser dataDividerByUser = new DataDividerByUser();
        Co_ocurrenceMatrixGenerator co_occurrenceMatrixGenerator = new Co_ocurrenceMatrixGenerator();
        AverageScoreEachUser averageScoreEachUser = new AverageScoreEachUser();
        MovieUserAverage movieUserAverage = new MovieUserAverage();
        NormalizeCo_occurrenceMatrix normalizeCo_occurrenceMatrix = new NormalizeCo_occurrenceMatrix();
        Multiplication multiplication = new Multiplication();
        MultiplicationSum multiplicationSum = new MultiplicationSum();
        RecommendationListGenerator generator = new RecommendationListGenerator();

        String[] path1 = {args[0], args[1]};
        String[] path2 = {args[1], args[2]};
        String[] path3 = {args[0], args[3]};
        String[] path4 = {args[3], args[4]};
        String[] path5 = {args[2], args[5]};
        String[] path6 = {args[5], args[0], args[4],args[6]};
		String[] path7 = {args[6], args[7]};
		String[] path8 = {args[7], args[8]};
		
        dataDividerByUser.main(path1);
        co_occurrenceMatrixGenerator.main(path2);
        AverageScoreEachUser.main(path3);
        movieUserAverage.main(path4);
        normalizeCo_occurrenceMatrix.main(path5);
        multiplication.main(path6);
        multiplicationSum.main(path7);
        generator.main(path8);
    }
}
