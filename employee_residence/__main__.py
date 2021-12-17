import yaml
import sys
import os
import logging


from employee_residence.modules import MetricsCalculationModule


def read_configuration_file(file_path):
    with open(file_path) as conf_file:
        config = yaml.load(conf_file, Loader=yaml.FullLoader)
    return config


def main():
    # read configuration file
    print("Reading config file...")
    configuration_file = os.path.join(os.getcwd(), sys.argv[1])
    config = read_configuration_file(configuration_file)
    print("Reading config done.")

    # initiating calculate metrics module
    print("Preparing calculate metrics module...")
    calculate_metrics_module = MetricsCalculationModule(config=config)

    # generating the statistics
    print("Calculate metrics module starting:")
    calculate_metrics_module.push_data_to_db()
    print("The closest employee to the company HQ :")
    print(calculate_metrics_module.calculate_closest_employee_to_company_HQ(), "\n")
    print("The employees living within 10km from the company HQ :")
    print(
        calculate_metrics_module.calculate_employees_living_within_radius_from_HQ(),
        "\n",
    )
    print("The three employees living on the wealthiest regions :")
    print(
        calculate_metrics_module.generate_list_of_employees_living_on_the_wealthiest_regions(),
        "\n",
    )
    print("Calculate metrics module ran successfully.")


if __name__ == "__main__":
    main()
