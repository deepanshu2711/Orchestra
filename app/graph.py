END = "__end__"

class Graph:
    def __init__(self,graph_id:str,version:str="v1") -> None:
        self.id, self.version = graph_id,version
        self.nodes,self.edges,self.entry = {},{},None

    def node(self,name,fn):
        self.nodes[name] = fn;
        return self

    def edge(self,a,b):
        self.edges[a] = b
        return self

    def route(self,a,router):
        self.edges[a] = router
        return self

    def start(self,name):
        self.entry = name
        return self
    
    def next_node(self,cur,state):
        e = self.edges[cur]
        return e(state)  if callable(e) else e 

    def run(self,state:dict) -> dict:
        cur = self.entry
        while cur != END:
            patch = self.nodes[cur](state) or {}
            state = {**state,**patch}
            cur = self.next_node(cur,state)
        return state
