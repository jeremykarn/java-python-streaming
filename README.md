To profile Python streaming (it will take ~5 minutes):

    scripts/compile_java.sh
    scripts/gen_test_data.sh
    scripts/profile_all_test_cases.sh profiles/my_changes_profile.txt

To run unit-tests:

    nosetests scripts/test_controller.py
