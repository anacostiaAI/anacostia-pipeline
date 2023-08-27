# Welcome to the Anacostia Pipeline!


# Example Usage

```python
n1 = TrueNode(name='n1')
n2 = FalseNode(name='n2')
n3 = TrueNode(name='n3', listen_to=n1 | n2)
n4 = TrueNode(name="n4", listen_to=n3)

p1 = Pipeline(nodes=[n1,n2,n3,n4])
```