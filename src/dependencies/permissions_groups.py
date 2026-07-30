from ganabosques_orm.enums.actions import Actions

class PermissionGroups:

    FARM = (
        Actions.API_FARMS,
        Actions.FRONT_FARMS,
        Actions.FRONT_REPORT,
    )

    ENTERPRISE = (
        Actions.API_ENTERPRISE,
        Actions.FRONT_ENTERPRISE,
        Actions.FRONT_REPORT,
    )

    ADM3 = (
        Actions.API_ADM,
        Actions.FRONT_ADM,
        Actions.FRONT_REPORT,
    )

    ANALYSIS = (
        *FARM,
        *ENTERPRISE,
        *ADM3,
    )
