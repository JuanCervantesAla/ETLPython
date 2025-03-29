from etl import ETLProcessor
import os

class Menu:
    def __init__(self):
        self.etl = ETLProcessor()

    def print_main_menu(self):
        # Muestra el menú principal.
        print("+------------------------------+")
        print("|            Menú              |")
        print("+------------------------------+")
        print("|1. Cargar datos desde Excel   |")
        print("|2. Cargar datos desde CSV     |")
        print("|3. Cargar datos desde JSON    |")
        print("|0. Salir                      |")
        print("+------------------------------+")

    def print_sub_menu(self):
        # Muestra el submenú de opciones después de cargar datos.
        print("+--------------------------------------------------+")
        print("|                    Submenú                       |")
        print("+--------------------------------------------------+")
        print("|1. Transformar y exportar datos                   |")
        print("|0. Volver al menú principal                       |")
        print("+--------------------------------------------------+")

    def print_export_menu(self):
        # Muestra las opciones de exportación.
        print("+------------------------+")
        print("|  Formatos disponibles  |")
        print("+------------------------+")
        print("|1. Excel (.xlsx)        |")
        print("|2. CSV (.csv)           |")
        print("|3. JSON (.json)         |")
        print("|4. SQL                  |")
        print("|0. Volver               |")
        print("+------------------------+")

    def run(self):
        # Ejecuta el programa mostrando los menús y manejando la lógica de interacción.
        while True:
            self.print_main_menu()
            option = input("Ingrese una opción: ")
            os.system('cls' if os.name == 'nt' else 'clear')

            match option:
                case "0":
                    print("Saliendo del programa...")
                    break
                case "1":
                    self.etl.load_data("excel")
                case "2":
                    self.etl.load_data("csv")
                case "3":
                    self.etl.load_data("json")
                case _:
                    print("Opción inválida. Intente nuevamente.")
                    continue

            if self.etl.data is not None:
                print("Vista previa de los datos:")
                print(self.etl.data.head())
                while True:
                    self.print_sub_menu()
                    sub_option = input("Ingrese una opción: ")
                    os.system('cls' if os.name == 'nt' else 'clear')
                    match sub_option:
                        case "0":
                            break

                        case "1":
                            while True:
                                self.print_export_menu()
                                export_option = input("Ingrese una opción: ")
                                os.system('cls' if os.name == 'nt' else 'clear')
                                match export_option:
                                    case "0":
                                        break
                                    case "1":
                                        file_name = input("Ingrese el nombre del archivo: ")
                                        # Llamar a las funciones de transformación sobre el dataset antes de exportar.
                                        #Sergio
                                        self.etl.normalize_dates()
                                        self.etl.convert_numeric_columns()
                                        self.etl.remove_accents()
                                        #Noel
                                        self.etl.convertir_reservation_date()
                                        self.etl.extraer_componentes_reservation_date()
                                        self.etl.extraer_nombre_dia()
                                        #Manuel
                                        i = 0
                                        for i in range(15):
                                            self.etl.fill_missing_values_all_interpolate()
                                            self.etl.fill_missing_values_all_ffill()
                                            self.etl.fill_missing_values_all_bfill()
                                        self.etl.remove_duplicates()

                                        #Juanjo
                                        self.etl.analizarReservaciones()
                                        self.etl.getTotalCost()
                                        self.etl.agregarColumnaCancelaciones()
                                        
                                        # Diego
                                        self.etl.data = self.etl.categorizar_lead_time(columna='lead_time')
                                        self.etl.data = self.etl.calcular_promedio_movil_lead_time(self.etl.data, ventana=7)
                                        self.etl.data = self.etl.crear_total_huespedes(self.etl.data)
                                        
                                        self.etl.export_data(file_name, "excel")
                                        
                                        
                                    case "2":
                                        file_name = input("Ingrese el nombre del archivo: ")
                                        # Llamar a las funciones de transformación sobre el dataset antes de exportar.
                                        #Sergio
                                        self.etl.normalize_dates()
                                        self.etl.convert_numeric_columns()
                                        self.etl.remove_accents()
                                        #Noel
                                        self.etl.convertir_reservation_date()
                                        self.etl.extraer_componentes_reservation_date()
                                        self.etl.extraer_nombre_dia()
                                        
                                        #Manuel
                                        i = 0
                                        for i in range(15):
                                            self.etl.fill_missing_values_all_interpolate()
                                            self.etl.fill_missing_values_all_ffill()
                                            self.etl.fill_missing_values_all_bfill()
                                        self.etl.remove_duplicates()
                                        
                                        #Juanjo
                                        self.etl.analizarReservaciones()
                                        self.etl.getTotalCost()
                                        self.etl.agregarColumnaCancelaciones()

                                        # Diego
                                        self.etl.data = self.etl.categorizar_lead_time(columna='lead_time')
                                        self.etl.data = self.etl.calcular_promedio_movil_lead_time(self.etl.data, ventana=7)
                                        self.etl.data = self.etl.crear_total_huespedes(self.etl.data)
                                        
                                        self.etl.export_data(file_name, "csv")
                                    case "3":
                                        file_name = input("Ingrese el nombre del archivo: ")
                                        # Llamar a las funciones de transformación sobre el dataset antes de exportar.
                                        #Sergio
                                        self.etl.normalize_dates()
                                        self.etl.convert_numeric_columns()
                                        self.etl.remove_accents()
                                        #Noel
                                        self.etl.convertir_reservation_date()
                                        self.etl.extraer_componentes_reservation_date()
                                        self.etl.extraer_nombre_dia()
                                        #Manuel
                                        i = 0
                                        for i in range(15):
                                            self.etl.fill_missing_values_all_interpolate()
                                            self.etl.fill_missing_values_all_ffill()
                                            self.etl.fill_missing_values_all_bfill()
                                        self.etl.remove_duplicates()

                                        #Juanjo
                                        self.etl.analizarReservaciones()
                                        self.etl.getTotalCost()
                                        self.etl.agregarColumnaCancelaciones()

                                        # Diego
                                        self.etl.data = self.etl.categorizar_lead_time(columna='lead_time')
                                        self.etl.data = self.etl.calcular_promedio_movil_lead_time(self.etl.data, ventana=7)
                                        self.etl.data = self.etl.crear_total_huespedes(self.etl.data)
                                        
                                        self.etl.export_data(file_name, "json")
                                    case "4":
                                        file_name = input("Ingrese el nombre del archivo: ")
                                        # Llamar a las funciones de transformación sobre el dataset antes de exportar.
                                        #Sergio
                                        self.etl.normalize_dates()
                                        self.etl.convert_numeric_columns()
                                        self.etl.remove_accents()
                                        #Noel
                                        self.etl.convertir_reservation_date()
                                        self.etl.extraer_componentes_reservation_date()
                                        self.etl.extraer_nombre_dia()
                                        #Manuel
                                        i = 0
                                        for i in range(15):
                                            self.etl.fill_missing_values_all_interpolate()
                                            self.etl.fill_missing_values_all_ffill()
                                            self.etl.fill_missing_values_all_bfill()
                                        self.etl.remove_duplicates()

                                        #Juanjo
                                        self.etl.analizarReservaciones()
                                        self.etl.getTotalCost()
                                        self.etl.agregarColumnaCancelaciones()

                                        # Diego
                                        self.etl.data = self.etl.categorizar_lead_time(columna='lead_time')
                                        self.etl.data = self.etl.calcular_promedio_movil_lead_time(self.etl.data, ventana=7)
                                        self.etl.data = self.etl.crear_total_huespedes(self.etl.data)
                                        
                                        self.etl.export_data(file_name, "sql")
                                    case _:
                                        print("Opción inválida. Intente nuevamente.")
                        case _:
                            print("Opción inválida. Intente nuevamente.")