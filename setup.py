"""
================================================================================
                                     SETUP                
================================================================================

Autor: Carlos Peraza
Versión: 1.0
Fecha: Agosto 2025

================================================================================
"""

from setuptools import setup, find_packages
import os

# Leer README para descripción larga
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# Leer requirements.txt para dependencias
def read_requirements():
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip() and not line.startswith("#")]

# Configuración del paquete
setup(
    name="wallapop-motos-scraper",
    version="1.0.0",
    author="Carlos Peraza",
    author_email="carlos@ejemplo.com",
    description="Sistema automatizado de scraping de motos en Wallapop con cálculo de rentabilidad",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/TU-USUARIO/wallapop-motos-scraper",
    project_urls={
        "Bug Tracker": "https://github.com/TU-USUARIO/wallapop-motos-scraper/issues",
        "Documentation": "https://github.com/TU-USUARIO/wallapop-motos-scraper#readme",
        "Source Code": "https://github.com/TU-USUARIO/wallapop-motos-scraper",
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: End Users/Desktop",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Environment :: Console",
        "Environment :: Web Environment",
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
        "testing": [
            "pytest>=7.4.0",
            "pytest-mock>=3.11.0",
            "responses>=0.23.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "wallapop-motos-scraper=main_runner:main",
            "run-scraper=main_runner:main",
            "run-all-models=run_all_models:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.yml", "*.yaml", "*.json", "*.txt", "*.md"],
    },
    zip_safe=False,
    keywords=[
        "wallapop", "scraping", "motos", "motorcycles", 
        "automation", "rentabilidad", "selenium", "google-sheets"
    ],
    platforms=["any"],
    license="MIT",
    
    # Metadata adicional
    maintainer="Carlos P",
    maintainer_email="7770cb2@gmail.com",
    download_url="https://github.com/TU-USUARIO/wallapop-motos-scraper/archive/v1.0.0.tar.gz",
    
    # Configuración específica
    options={
        "bdist_wheel": {
            "universal": False
        }
    }
)

# ============================================================================
# FUNCIONES DE POST-INSTALACIÓN
# ============================================================================

def post_install():
    """Funciones a ejecutar después de la instalación"""
    print(" Wallapop Motos Scraper instalado correctamente!")
    print("\n Próximos pasos:")
    print("1. Configurar Google Cloud y obtener credenciales")
    print("2. Crear archivo .env con las variables necesarias")
    print("3. Probar conexión: wallapop-motos-scraper --test-connection")
    print("4. Ejecutar scraper: wallapop-motos-scraper cb125r --test")
    print("\n Documentación completa en README.md")

if __name__ == "__main__":
    # Ejecutar post-instalación si se ejecuta directamente
    post_install()
