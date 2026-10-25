from app.graph import END, Graph

def main():
    graph = Graph("demo")

    graph.node("a",lambda s : {"first":1})
    graph.node("b",lambda s : {"secound":2})

    graph.edge("a","b")
    graph.edge("b",END)

    graph.start("a")
    graph.validate()

    print(graph.run({}))

if __name__ == "__main__":
    main()
