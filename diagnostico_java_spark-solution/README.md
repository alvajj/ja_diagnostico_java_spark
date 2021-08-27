# Examen Diagnostico JAVA to Big Data
**cuentas con 24 horas para resolver el ejercicio**


## Instrucciones

1. Realizar un fork de este repositorio a tu cuenta de github.
2. Crear una rama que por nombre lleve tus iniciales a partir de la rama solution.
3. Realizar los ejercicios solicitados abajo.
4. Enviar por correo electronico la notificación de finalización y el link al repositorio de solución.
**_No debes hacer PULL REQUEST_**
## ¿Qué evaluaremos?

* Resuelva con el API de SparkSQL el ejercicio planteado.
* El uso de sentencias SQL queda estrictamente prohibido.
* El uso de cadenas en las clases que implementan la lógica de solución están muy mal vistos por nuestra area de QA, sea
  cuidadoso.
* Amamos las pruebas de calidad, es necesario que el método 4 cuente con una prueba unitaria 
    que valide que se han filtrado correctamente los jugadores.
* Modularice sú código lo suficiente de tal forma que cada método haga una sola cosa.
* Hemos soñado con poder leer las rutas de entrada y salida desde un archivo de configuración, sería increible tener
  uno! (leer el archivo params)

## Ejercicio

1. La tabla de salida debe contener las siguientes columnas:
   `short_name, long_name, age, height_cm, weight_kg, nationality, club_name, overall, potential, team_position`
2. Agregar una columna `player_cat` que responderá a la siguiente regla (rank over Window particionada por `nationality` y `team_position`
   y ordenada por `overall`):
    * **A** si el jugador es de los mejores 3 jugadores en su posición de su país.
    * **B** si el jugador es de los mejores 5 jugadores en su posición de su país.
    * **C** si el jugador es de los mejores 10 jugadores en su posición de su país.
    * **D** para el resto de jugadores.

   ***tip** para resolver este ejercicio, mire el método de ejemplo exampleWindowFunction incluido en el código.*
3. Agregaremos una columna `potential_vs_overall` con la siguiente regla:
    * Columna `potential` dividida por la columna `overall`
4. Filtraremos de acuerdo a las columnas `player_cat` y `potential_vs_overall` con las siguientes condiciones:
    * Si `player_cat` esta en los siguientes valores: **A**, **B**
    * Si `player_cat` es **C** y `potential_vs_overall` es superior a **1.15**
    * Si `player_cat` es **D** y `potential_vs_overall` es superior a **1.25**
5. Agregar un parametro al archivo params que de ser 1 realice todos los pasos únicamente para los jugadores menores de 23 años 
    y en caso de ser 0 que lo haga con todos los jugadores del dataset.
6. Por favor escriba la tabla resultante de los pasos anteriores particionada por la columna `nationality`, la salida
   debe estar escrita en formato **parquet** y debe usarse el método `coalese(1)`
   para obtener solo un archivo por partición.

¡Buena suerte!
