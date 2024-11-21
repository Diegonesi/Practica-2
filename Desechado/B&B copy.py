class Nodo:
    def __init__(self, nivel, beneficio, peso, bound, incluido):
        self.nivel = nivel
        self.beneficio = beneficio
        self.peso = peso
        self.bound = bound
        self.incluido = incluido

def bound(nodo, n, W, beneficios, pesos):
    if nodo.peso >= W:
        return 0
    beneficio_bound = nodo.beneficio
    j = nodo.nivel + 1
    peso_total = nodo.peso
    while j < n and peso_total + pesos[j] <= W:
        peso_total += pesos[j]
        beneficio_bound += beneficios[j]
        j += 1
    if j < n:
        beneficio_bound += (W - peso_total) * beneficios[j] / pesos[j]
    return beneficio_bound

def branch_and_bound(n, W, beneficios, pesos):
    cola = []
    nodo_u = Nodo(-1, 0, 0, 0.0, [])
    nodo_v = Nodo(0, 0, 0, 0.0, [])
    nodo_u.bound = bound(nodo_u, n, W, beneficios, pesos)
    max_beneficio = 0
    cola.append(nodo_u)
    while cola:
        nodo_u = cola.pop(0)
        if nodo_u.nivel == -1:
            nodo_v.nivel = 0
        if nodo_u.nivel == n - 1:
            continue
        nodo_v.nivel = nodo_u.nivel + 1
        nodo_v.peso = nodo_u.peso + pesos[nodo_v.nivel]
        nodo_v.beneficio = nodo_u.beneficio + beneficios[nodo_v.nivel]
        nodo_v.incluido = nodo_u.incluido + [nodo_v.nivel]
        if nodo_v.peso <= W and nodo_v.beneficio > max_beneficio:
            max_beneficio = nodo_v.beneficio
        nodo_v.bound = bound(nodo_v, n, W, beneficios, pesos)
        if nodo_v.bound > max_beneficio:
            cola.append(nodo_v)
        nodo_v = Nodo(nodo_v.nivel, nodo_u.beneficio, nodo_u.peso, 0.0, nodo_u.incluido)
        nodo_v.bound = bound(nodo_v, n, W, beneficios, pesos)
        if nodo_v.bound > max_beneficio:
            cola.append(nodo_v)
    return max_beneficio

# Ejemplo de uso
beneficios = [60, 100, 120]
pesos = [10, 20, 30]
W = 50
n = len(beneficios)
max_beneficio = branch_and_bound(n, W, beneficios, pesos)
print(f"El beneficio máximo es {max_beneficio}")
