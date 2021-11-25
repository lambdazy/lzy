import { createContext } from "react";
import { useState } from "react";
import { useContext } from "react";
import { FC } from "react";
import { Alert } from "react-bootstrap";

export interface AlertPatrams {
  show: boolean;
  text: string | undefined;
  header: string | undefined;
  variant: "danger" | "success";
  onClose: (() => void) | undefined;
}

export interface AlertContext {
  params: AlertPatrams;
  show: (
    text: string,
    header: string,
    onClose: (() => void) | undefined,
    variant: "danger" | "success"
  ) => void;
  close: () => void;
}

const alertContext = createContext<AlertContext>({
  params: {
    show: false,
    text: undefined,
    header: undefined,
    onClose: undefined,
    variant: "danger"
  },
  show: () => {},
  close: () => {},
});

export function useAlert(): AlertContext {
  return useContext(alertContext);
}

export function useProvideAlert(): AlertContext {
  const [showState, setShowState] = useState<AlertPatrams>({
    show: false,
    text: undefined,
    header: undefined,
    onClose: undefined,
    variant: "danger"
  });
  const show = (
    text: string,
    header: string,
    onClose: (() => void) | undefined,
    variant: "danger"| "success"
  ) => {
    if (!showState.show)
      setShowState({
        show: true,
        text,
        header,
        onClose,
        variant
      });
  };
  const close = () => {
    if (showState.show)
      setShowState({
        show: false,
        text: undefined,
        header: undefined,
        onClose: undefined,
        variant: "danger"
      });
  };
  return { params: showState, show, close };
}

export function ProvideAlert(props: { children: any }) {
  const auth = useProvideAlert();
  return (
    <alertContext.Provider value={auth}>{props.children}</alertContext.Provider>
  );
}

export const ErrorAlert: FC<{}> = () => {
  let alert = useAlert();
  return (
    <Alert
      show={alert.params.show}
      onClose={() => {
        alert.close();
        if (alert.params.onClose !== undefined) alert.params.onClose();
      }}
      variant={alert.params.variant}
      dismissible
    >
      <Alert.Heading>{alert.params.header}</Alert.Heading>
      <p>{alert.params.text}</p>
    </Alert>
  );
};
