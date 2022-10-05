import {createContext, FC, useContext, useState} from "react";
import {Alert} from "react-bootstrap";


export enum AlertVariant {
    DANGER = "danger",
    SUCCESS = "success"
}

export interface AlertParams {
    show: boolean;
    text: string | undefined;
    header: string | undefined;
    variant: AlertVariant | undefined;
}

export interface AlertContext {
    params: AlertParams;
    showDanger: (
        header: string,
        text: string
    ) => void;
    showSuccess: (
        header: string,
        text: string
    ) => void;
    close: () => void;
}

const alertContext = createContext<AlertContext>({
    params: {
        show: false,
        text: undefined,
        header: undefined,
        variant: undefined
    },
    showDanger: () => {},
    showSuccess: () => {},
    close: () => {}
});

export function useAlert(): AlertContext {
    return useContext(alertContext);
}

export function useProvideAlert(): AlertContext {
    const [showState, setShowState] = useState<AlertParams>({
        show: false,
        text: undefined,
        header: undefined,
        variant: undefined
    });
    const show = (
        text: string,
        header: string,
        variant: AlertVariant
    ) => {
        if (!showState.show) {
            setShowState({
                show: true,
                text,
                header,
                variant
            });
        }
    }
    const showDanger = (
        text: string,
        header: string
    ) => {
        show(text, header, AlertVariant.DANGER);
    };
    const showSuccess = (
        text: string,
        header: string
    ) => {
        show(text, header, AlertVariant.SUCCESS)
    }
    const close = () => {
        if (showState.show)
            setShowState({
                show: false,
                text: undefined,
                header: undefined,
                variant: undefined
            });
    };
    return {params: showState, showSuccess, showDanger, close};
}

export function ProvideAlert(props: { children: any }) {
    const auth = useProvideAlert();
    return (
        <alertContext.Provider value={auth}>{props.children}</alertContext.Provider>
    );
}

export const ErrorAlert: FC = () => {
    let alert = useAlert();
    return (
        <Alert
            show={alert.params.show}
            onClose={() => {
                alert.close();
            }}
            variant={alert.params.variant}
            dismissible
        >
            <Alert.Heading>{alert.params.header}</Alert.Heading>
            <p>{alert.params.text}</p>
        </Alert>
    );
};
