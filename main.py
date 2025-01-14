import random
from collections import deque

# Оцінка кількості неправильних позицій
def h1(state, goal_state):
    return sum(1 for i in range(len(state)) if state[i] != goal_state[i] and state[i] != 0)

# Генерація сусідів
def get_neighbors(state):
    neighbors = []
    zero_index = state.index(0)
    row, col = divmod(zero_index, 3)
    moves = [(-1, 0), (1, 0), (0, -1), (0, 1)]  # Вгору, вниз, вліво, вправо

    for dr, dc in moves:
        new_row, new_col = row + dr, col + dc
        if 0 <= new_row < 3 and 0 <= new_col < 3:
            new_index = new_row * 3 + new_col
            new_state = list(state)
            new_state[zero_index], new_state[new_index] = new_state[new_index], new_state[zero_index]
            neighbors.append(tuple(new_state))
    return neighbors

# Алгоритм BFS
def bfs_solve(initial_state, goal_state):
    queue = deque([(initial_state, [])])
    visited = set()
    generated_nodes = 0
    max_memory = 0

    while queue:
        state, path = queue.popleft()
        if state in visited:
            continue
        visited.add(state)
        max_memory = max(max_memory, len(queue) + len(visited))
        if state == goal_state:
            return path, generated_nodes, max_memory
        for neighbor in get_neighbors(state):
            if neighbor not in visited:
                queue.append((neighbor, path + [neighbor]))
                generated_nodes += 1
    return None, generated_nodes, max_memory

# Алгоритм A*
def a_star_solve(initial_state, goal_state):
    open_set = [(h1(initial_state, goal_state), 0, initial_state, [])]
    visited = set()
    generated_nodes = 0
    max_memory = 0

    while open_set:
        _, cost, state, path = open_set.pop(0)
        if state in visited:
            continue
        visited.add(state)
        max_memory = max(max_memory, len(open_set) + len(visited))
        if state == goal_state:
            return path, generated_nodes, max_memory
        for neighbor in get_neighbors(state):
            if neighbor not in visited:
                new_cost = cost + 1
                heuristic = h1(neighbor, goal_state)
                open_set.append((new_cost + heuristic, new_cost, neighbor, path + [neighbor]))
                generated_nodes += 1
        open_set.sort()
    return None, generated_nodes, max_memory

# Алгоритм Backtracking
def backtrack_solve(state, goal_state, path=None, visited=None, max_depth=50):
    if path is None:
        path = []
    if visited is None:
        visited = set()
    if len(path) > max_depth:
        return None, 0, len(visited)  # Гранична глибина
    if state == goal_state:
        return path, len(visited), len(visited)

    visited.add(state)
    max_memory = len(visited)
    for neighbor in sorted(get_neighbors(state), key=lambda s: h1(s, goal_state)):
        if neighbor not in visited:
            result, generated, memory = backtrack_solve(neighbor, goal_state, path + [neighbor], visited, max_depth)
            if result is not None:
                return result, generated + 1, max(max_memory, memory)
    return None, len(visited), max_memory

# Запуск експериментів
def run_experiments():
    goal_state = tuple(range(9))  # Цільовий стан: 0, 1, 2, 3, 4, 5, 6, 7, 8
    initial_states = [tuple(random.sample(range(9), 9)) for _ in range(20)]

    results = []
    for state in initial_states:
        print(f"Initial state: {state}")
        for algorithm_name, algorithm in [
            ("BFS", bfs_solve),
            ("A*", a_star_solve),
            ("Backtracking", lambda s, g: backtrack_solve(s, g, max_depth=50))
        ]:
            print(f"Running {algorithm_name}...")
            solution, generated, memory = algorithm(state, goal_state)
            iterations = len(solution) if solution else 0
            results.append({
                "algorithm": algorithm_name,
                "initial_state": state,
                "iterations": iterations,
                "generated": generated,
                "memory": memory,
            })
    return results

# Формування таблиць
def generate_table(results, algorithm_name):
    print(f"\nТаблиця характеристик для {algorithm_name}")
    print("Початкові стани\tІтерації\tВсього вузлів\tВсього вузлів у пам’яті")
    for i, result in enumerate(results):
        if result['algorithm'] == algorithm_name:
            state = result['initial_state']
            iterations = result['iterations']
            generated = result['generated']
            memory = result['memory']
            print(f"Стан {i+1}\t{iterations}\t{generated}\t{memory}")

# Головний блок
if __name__ == "__main__":
    results = run_experiments()

    for algorithm in ["BFS", "A*", "Backtracking"]:
        generate_table(results, algorithm)
