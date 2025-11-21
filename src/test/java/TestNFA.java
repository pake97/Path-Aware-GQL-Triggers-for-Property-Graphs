import java.util.*;


public class TestNFA {

        private static class Edge {
            public String label;
            public int inputId;
            public int sourceId;
            public int destId;

            public Edge(String input, int inputId, int sourceId, int destId) {

                this.destId = destId;
                this.sourceId = sourceId;
                this.label=input;
                this.inputId= inputId;
            }
        }



    public static class AutomatonManager {
        private final Map<String, List<Integer>> labelToStatesMap = new HashMap<>();

        public AutomatonManager(Map<Integer, String> automaton) {
            for (Map.Entry<Integer, String> entry : automaton.entrySet()) {
                Integer state = entry.getKey();
                String label = entry.getValue();
                labelToStatesMap
                        .computeIfAbsent(label, k -> new ArrayList<>())
                        .add(state);
            }
        }

        // Get the list of states for a given label
        public List<Integer> getStatesForLabel(String label) {
            return labelToStatesMap.getOrDefault(label, Collections.emptyList());
        }

        // Optional: get the full reversed map
        public Map<String, List<Integer>> getLabelToStatesMap() {
            return Collections.unmodifiableMap(labelToStatesMap);
        }
    }



    public static void main(String[] args) {


            // Complexity = transaction.size()*activeVariables.size()*parsing
            //RPQ AB[1,3]C


            Map<Integer,String> automaton1 = Map.of(1,"A",2,"B",3,"C");
            Map<Integer,String> automaton2 = Map.of(1,"A",2,"B",3,"B",4,"C");
            Map<Integer,String> automaton3 = Map.of(1,"A",2,"B",3,"B",4,"B",5,"C");

            List<Map<Integer,String>> automata = List.of(automaton1,automaton2,automaton3);


            Edge edge1 = new Edge("A",23,22,25);
            Edge edge2 = new Edge("B",43,84,84);
            List<Edge> transaction = List.of(edge1,edge2);



            List<Map<Integer,String>> queries = new ArrayList<>();


            for(Edge e : transaction){
                int inputIdNew = e.inputId;
                String label = e.label;

                for(Map<Integer,String> automaton : automata) {
                    AutomatonManager am = new AutomatonManager(automaton);
                    List<Integer> positions = am.getStatesForLabel(label);

                    if(queries.size()==0) {
                        for (int i = 0; i < positions.size(); i++) {
                            queries.add(new HashMap<>());
                        }
                    }
                    else{
                        List<Map<Integer,String>> toAdd = new ArrayList<>();
                        for(Map<Integer,String> query: queries){
                            toAdd.add(new HashMap<>(query));
                        }
                        queries=toAdd;
                    }


                        for (Map.Entry<Integer, String> entry : automaton.entrySet()) {
                            Integer key = entry.getKey();

                            // find funcion implemented by ASS
                            int pos = -1;

                            for (int i = 0; i < positions.size(); i++) {
                                if (positions.get(i).equals(key)) {
                                    pos = i;
                                    break;
                                }
                            }


                            if (pos >= 0) {
                                for (Map<Integer,String> query: queries) {
                                    query.put(key, e.inputId + "");
                                }
                            } else {
                                for (Map<Integer,String> query: queries) {
                                    query.put(key, label);
                                }
                            }

                        }

                    }






                    }







        for(Map<Integer,String> query :queries){
            System.out.println("-----------");
            for (Map.Entry<Integer, String> entry : query.entrySet()){
                System.out.println(entry.getValue());
            }
        }



        }



    }




