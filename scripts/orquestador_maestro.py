#!/usr/bin/env python3
"""
ORQUESTADOR MAESTRO - PIPELINE COMPLETO DE DATA WAREHOUSE
=========================================================
Ejecuta todo el pipeline del Data Warehouse de manera secuencial y coordinada:

1. Construcción de Dimensiones (build_all_dimensions.py) - INCLUYE dim_inventario
2. Construcción de Fact Table (build_fact_ventas.py)  
3. Configuración de Base de Datos (setup_database.py)
4. Validación y Reporte Final

Incluye manejo de errores, logging detallado y capacidad de re-ejecución.
Para modelo dimensional completo - Tesis de grado.
"""

import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

# Configuracin
ROOT = Path(__file__).parent.parent
CONFIG_DIR = ROOT / "config"
SCRIPTS_DIR = ROOT / "scripts"
LOGS_DIR = ROOT / "logs"

# Crear directorio de logs
LOGS_DIR.mkdir(exist_ok=True)

load_dotenv(CONFIG_DIR / ".env")

class DataWarehouseOrchestrator:
    """Orquestador principal del Data Warehouse"""
    
    def __init__(self):
        self.start_time = time.time()
        self.log_file = LOGS_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.steps_completed = []
        self.errors = []
        
    def log(self, message, level="INFO"):
        """Logging unificado"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {level}: {message}"
        
        # Imprimir en consola
        print(log_entry)
        
        # Guardar en archivo
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry + "\n")
    
    def run_script(self, script_name, description):
        """Ejecuta un script de Python y maneja errores"""
        self.log(f" INICIANDO: {description}")
        
        script_path = SCRIPTS_DIR / script_name
        
        if not script_path.exists():
            error_msg = f"Script no encontrado: {script_path}"
            self.log(error_msg, "ERROR")
            self.errors.append(error_msg)
            return False
        
        try:
            # Ejecutar script
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                cwd=ROOT
            )
            
            if result.returncode == 0:
                self.log(f" COMPLETADO: {description}")
                self.log(f"Output: {result.stdout.strip()}")
                self.steps_completed.append(script_name)
                return True
            else:
                error_msg = f"Error ejecutando {script_name}: {result.stderr}"
                self.log(error_msg, "ERROR")
                self.log(f"Output: {result.stdout}")
                self.errors.append(error_msg)
                return False
                
        except Exception as e:
            error_msg = f"Excepcin ejecutando {script_name}: {e}"
            self.log(error_msg, "ERROR")
            self.errors.append(error_msg)
            return False
    
    def check_prerequisites(self):
        """Verifica pre-requisitos antes de iniciar"""
        self.log(" Verificando pre-requisitos...")
        
        # Verificar archivos de configuracin
        env_file = CONFIG_DIR / ".env"
        if not env_file.exists():
            self.log(" Archivo .env no encontrado", "ERROR")
            return False
        
        # Verificar conexin a OroCommerce
        try:
            oro_conn = psycopg2.connect(
                host=os.getenv('ORO_DB_HOST'),
                port=int(os.getenv('ORO_DB_PORT')),
                dbname=os.getenv('ORO_DB_NAME'),
                user=os.getenv('ORO_DB_USER'),
                password=os.getenv('ORO_DB_PASS')
            )
            oro_conn.close()
            self.log(" Conexin a OroCommerce OK")
        except Exception as e:
            self.log(f" Error conectando a OroCommerce: {e}", "ERROR")
            return False
        
        # Verificar scripts necesarios
        required_scripts = [
            'build_all_dimensions.py',
            'build_fact_ventas.py', 
            'setup_database.py'
        ]
        
        for script in required_scripts:
            if not (SCRIPTS_DIR / script).exists():
                self.log(f" Script requerido no encontrado: {script}", "ERROR")
                return False
        
        self.log(" Todos los pre-requisitos verificados")
        return True
    
    def execute_pipeline(self):
        """Ejecuta el pipeline completo"""
        self.log("  INICIANDO PIPELINE COMPLETO DE DATA WAREHOUSE")
        self.log("=" * 70)
        
        # Pipeline steps
        pipeline_steps = [
            ("build_all_dimensions.py", "Construcción de todas las dimensiones (incluye inventario)"),
            ("build_fact_ventas.py", "Construcción de tabla de hechos fact_ventas"),
            ("setup_database.py", "Configuración completa de base de datos")
        ]
        
        success_count = 0
        
        for script_name, description in pipeline_steps:
            step_start = time.time()
            
            if self.run_script(script_name, description):
                step_duration = time.time() - step_start
                self.log(f"  Duracin del paso: {step_duration:.1f} segundos")
                success_count += 1
            else:
                self.log(f" Pipeline detenido en: {description}", "ERROR")
                break
            
            self.log("-" * 50)
        
        return success_count == len(pipeline_steps)
    
    def generate_final_report(self):
        """Genera reporte final del pipeline"""
        self.log(" GENERANDO REPORTE FINAL...")
        
        try:
            # Conectar a DW para estadsticas
            dw_conn = psycopg2.connect(
                host=os.getenv('DW_ORO_DB_HOST'),
                port=int(os.getenv('DW_ORO_DB_PORT')),
                dbname=os.getenv('DW_ORO_DB_NAME'),
                user=os.getenv('DW_ORO_DB_USER'),
                password=os.getenv('DW_ORO_DB_PASS')
            )
            
            with dw_conn.cursor() as cur:
                # Contar tablas
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                table_count = cur.fetchone()[0]
                
                # Estadsticas de fact_ventas
                cur.execute("SELECT COUNT(*) FROM fact_ventas")
                fact_count = cur.fetchone()[0]
                
                cur.execute("SELECT SUM(total_linea) FROM fact_ventas")
                total_sales = cur.fetchone()[0] or 0
                
                # Rango de fechas
                cur.execute("SELECT MIN(id_fecha), MAX(id_fecha) FROM fact_ventas")
                date_range = cur.fetchone()
                
            dw_conn.close()
            
            self.log(" PIPELINE COMPLETADO EXITOSAMENTE!")
            self.log("=" * 50)
            self.log(f" Tablas creadas: {table_count}")
            self.log(f" Transacciones cargadas: {fact_count:,}")
            self.log(f" Total ventas: ${total_sales:,.2f}")
            self.log(f" Rango fechas: {date_range[0]} - {date_range[1]}")
            
            return True
            
        except Exception as e:
            self.log(f"  Error generando reporte final: {e}", "ERROR")
            return False
    
    def cleanup_on_error(self):
        """Limpieza en caso de error"""
        if self.errors:
            self.log(" Ejecutando limpieza por errores...")
            # Aqu se pueden agregar tareas de limpieza especficas
            self.log(" Limpieza completada")
    
    def run(self):
        """Mtodo principal para ejecutar todo el orquestador"""
        self.log(f" INICIANDO ORQUESTADOR DE DATA WAREHOUSE")
        self.log(f" Directorio de trabajo: {ROOT}")
        self.log(f" Log del pipeline: {self.log_file}")
        self.log("")
        
        try:
            # Verificar pre-requisitos
            if not self.check_prerequisites():
                self.log(" Pre-requisitos no cumplidos", "ERROR")
                return False
            
            # Ejecutar pipeline
            pipeline_success = self.execute_pipeline()
            
            if pipeline_success:
                # Generar reporte final
                report_success = self.generate_final_report()
                
                # Estadsticas finales
                total_duration = time.time() - self.start_time
                self.log(f"  TIEMPO TOTAL: {total_duration:.1f} segundos")
                self.log(f" PASOS COMPLETADOS: {len(self.steps_completed)}")
                
                if report_success:
                    self.log(" DATA WAREHOUSE LISTO PARA PRODUCCIN!")
                    return True
                else:
                    self.log("  Pipeline completado pero con errores en reporte", "WARNING")
                    return True
            else:
                self.log(f" PIPELINE FALL - {len(self.errors)} errores", "ERROR")
                self.cleanup_on_error()
                return False
                
        except Exception as e:
            self.log(f" ERROR CRTICO EN ORQUESTADOR: {e}", "ERROR")
            self.cleanup_on_error()
            return False

def main():
    """Funcin principal"""
    print(" ORQUESTADOR MAESTRO DE DATA WAREHOUSE")
    print(" PuntaFina - Anlisis de OroCommerce")
    print("=" * 60)
    
    orchestrator = DataWarehouseOrchestrator()
    success = orchestrator.run()
    
    if success:
        print(f"\n XITO! Data Warehouse completamente configurado")
        print(f" Log completo: {orchestrator.log_file}")
        exit(0)
    else:
        print(f"\n FALL - Revisar log: {orchestrator.log_file}")
        exit(1)

if __name__ == "__main__":
    main()
