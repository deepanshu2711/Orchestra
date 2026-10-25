

#NOTE:  lambda is a way to create small , anonomus function in python
#lambda arguments: expression

from app.graph import END, Graph


g = (Graph("toy")
     .node("write",    lambda s: {"draft": s.get("draft","") + "x", "rounds": s.get("rounds",0)+1})
     .node("critique", lambda s: {"ok": len(s["draft"]) >= 3})
     .route("write",    lambda s: "critique")
     .route("critique", lambda s: END if s["ok"] or s["rounds"] >= 3 else "write")
     .start("write")
     .validate()
     )


print(g.run({}))
